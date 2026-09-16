package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web/*
var embeddedWeb embed.FS

const (
	protocolMagic   = "CHUSAN-LAN-LAB"
	protocolVersion = 1
	maxEvents       = 1000
)

type ServiceName string

const (
	serviceParty     ServiceName = "party"
	serviceSetting   ServiceName = "setting"
	serviceAdvertise ServiceName = "advertise"
	serviceLanBeacon ServiceName = "lan_beacon"
	serviceLanSync   ServiceName = "lan_sync"
)

var servicePorts = map[ServiceName]int{
	serviceParty:     50200,
	serviceSetting:   50201,
	serviceAdvertise: 50202,
	serviceLanBeacon: 40112,
	serviceLanSync:   40110,
}

type Config struct {
	NodeName      string   `json:"node_name"`
	Group         string   `json:"group"`
	GroupRole     string   `json:"group_role"`
	LANRole       string   `json:"lan_role"`
	CabinetMode   string   `json:"cabinet_mode"`
	BindIP        string   `json:"bind_ip"`
	Targets       []string `json:"targets"`
	AutoReply     bool     `json:"auto_reply"`
	AutoJoin      bool     `json:"auto_join"`
	EventMode     int      `json:"event_mode"`
	MusicID       int      `json:"music_id"`
	UDPParty      bool     `json:"udp_party"`
	UDPSetting    bool     `json:"udp_setting"`
	UDPAdvertise  bool     `json:"udp_advertise"`
	TCPParty      bool     `json:"tcp_party"`
	TCPSetting    bool     `json:"tcp_setting"`
	UDPLanInstall bool     `json:"udp_lan_install"`
	TCPLanInstall bool     `json:"tcp_lan_install"`
}

type LabMessage struct {
	Magic         string   `json:"magic"`
	Version       int      `json:"version"`
	Type          string   `json:"type"`
	InstanceID    string   `json:"instance_id"`
	NodeName      string   `json:"node_name"`
	Group         string   `json:"group"`
	Service       string   `json:"service"`
	Role          string   `json:"role,omitempty"`
	HostIPv4      string   `json:"host_ipv4,omitempty"`
	EventMode     int      `json:"event_mode,omitempty"`
	MusicID       int      `json:"music_id,omitempty"`
	Result        string   `json:"result,omitempty"`
	Members       int      `json:"members,omitempty"`
	ProbeID       string   `json:"probe_id"`
	TimestampNS   int64    `json:"timestamp_ns"`
	EchoNS        int64    `json:"echo_ns,omitempty"`
	GroupMatch    *bool    `json:"group_match,omitempty"`
	AdvertisedIPs []string `json:"advertised_ipv4,omitempty"`
	Padding       string   `json:"padding,omitempty"`
}

type Event struct {
	ID         int64   `json:"id"`
	Time       string  `json:"time"`
	Level      string  `json:"level"`
	Direction  string  `json:"direction,omitempty"`
	Transport  string  `json:"transport,omitempty"`
	Service    string  `json:"service,omitempty"`
	Local      string  `json:"local,omitempty"`
	Remote     string  `json:"remote,omitempty"`
	Kind       string  `json:"kind"`
	NodeName   string  `json:"node_name,omitempty"`
	Group      string  `json:"group,omitempty"`
	GroupMatch *bool   `json:"group_match,omitempty"`
	ProbeID    string  `json:"probe_id,omitempty"`
	RTTMS      float64 `json:"rtt_ms,omitempty"`
	Size       int     `json:"size,omitempty"`
	Message    string  `json:"message"`
}

type EventStore struct {
	mu          sync.Mutex
	events      []Event
	subscribers map[chan Event]struct{}
	nextID      int64
}

func newEventStore() *EventStore {
	return &EventStore{subscribers: make(map[chan Event]struct{})}
}

func (s *EventStore) add(event Event) {
	s.mu.Lock()
	s.nextID++
	event.ID = s.nextID
	if event.Time == "" {
		event.Time = time.Now().Format(time.RFC3339Nano)
	}
	s.events = append(s.events, event)
	if len(s.events) > maxEvents {
		s.events = append([]Event(nil), s.events[len(s.events)-maxEvents:]...)
	}
	for subscriber := range s.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
	s.mu.Unlock()
}

func (s *EventStore) snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

func (s *EventStore) clear() {
	s.mu.Lock()
	s.events = nil
	s.mu.Unlock()
}

func (s *EventStore) subscribe() (<-chan Event, func()) {
	channel := make(chan Event, 64)
	s.mu.Lock()
	s.subscribers[channel] = struct{}{}
	s.mu.Unlock()
	return channel, func() {
		s.mu.Lock()
		delete(s.subscribers, channel)
		close(channel)
		s.mu.Unlock()
	}
}

type App struct {
	mu                sync.Mutex
	running           bool
	config            Config
	startedAt         time.Time
	instanceID        string
	udp               map[ServiceName]*net.UDPConn
	tcp               map[ServiceName]*net.TCPListener
	stopCh            chan struct{}
	wg                sync.WaitGroup
	events            *EventStore
	pendingMu         sync.Mutex
	pending           map[string]time.Time
	statsMu           sync.Mutex
	stats             map[string]uint64
	lanMu             sync.Mutex
	lanServers        map[string]string
	flowMu            sync.Mutex
	phase             string
	lanReady          bool
	settingReady      bool
	advertiseReady    bool
	settingStarted    bool
	lanConnecting     bool
	settingConnecting bool
	settingConnected  bool
	recruiting        bool
	recruitID         string
	recruitCancel     chan struct{}
	invites           map[string]LabMessage
	members           map[string]string
}

func newApp() *App {
	return &App{
		instanceID: randomID(),
		udp:        make(map[ServiceName]*net.UDPConn),
		tcp:        make(map[ServiceName]*net.TCPListener),
		events:     newEventStore(),
		pending:    make(map[string]time.Time),
		stats:      make(map[string]uint64),
		lanServers: make(map[string]string),
		phase:      "stopped",
		invites:    make(map[string]LabMessage),
		members:    make(map[string]string),
	}
}

func defaultConfig() Config {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "chusan-lab-node"
	}
	return Config{
		NodeName:      hostname,
		Group:         "A",
		GroupRole:     "parent",
		LANRole:       "server",
		CabinetMode:   "SP",
		BindIP:        "0.0.0.0",
		Targets:       []string{"255.255.255.255"},
		AutoReply:     true,
		AutoJoin:      false,
		EventMode:     0,
		MusicID:       0,
		UDPParty:      true,
		UDPSetting:    true,
		UDPAdvertise:  true,
		TCPParty:      true,
		TCPSetting:    true,
		UDPLanInstall: true,
		TCPLanInstall: true,
	}
}

func normalizeConfig(config Config) (Config, error) {
	config.NodeName = strings.TrimSpace(config.NodeName)
	if config.NodeName == "" || len(config.NodeName) > 64 {
		return Config{}, errors.New("节点名称必须为 1～64 个字符")
	}
	config.Group = strings.ToUpper(strings.TrimSpace(config.Group))
	if config.Group != "OFF" && config.Group != "A" && config.Group != "B" && config.Group != "C" && config.Group != "D" {
		return Config{}, errors.New("组别必须是 OFF、A、B、C 或 D")
	}
	config.GroupRole = strings.ToLower(strings.TrimSpace(config.GroupRole))
	if config.Group == "OFF" {
		if config.GroupRole != "off" {
			return Config{}, errors.New("组别为 OFF 时，柜机身份必须为 Off")
		}
	} else if config.GroupRole != "parent" && config.GroupRole != "child" {
		return Config{}, errors.New("组别为 A～D 时，柜机身份必须是 Parent 或 Child")
	}
	config.LANRole = strings.ToLower(strings.TrimSpace(config.LANRole))
	if config.LANRole != "server" && config.LANRole != "client" {
		return Config{}, errors.New("LAN Install 身份必须是 Server 或 Client")
	}
	config.CabinetMode = strings.ToUpper(strings.TrimSpace(config.CabinetMode))
	if config.CabinetMode != "SP" && config.CabinetMode != "CVT" {
		return Config{}, errors.New("框体模式必须是 SP 或 CVT")
	}
	if config.EventMode < 0 || config.EventMode > 255 {
		return Config{}, errors.New("Event Mode 必须为 0～255")
	}
	if config.MusicID < 0 {
		return Config{}, errors.New("曲目 ID 不能为负数")
	}
	config.BindIP = strings.TrimSpace(config.BindIP)
	if config.BindIP == "" {
		config.BindIP = "0.0.0.0"
	}
	bindIP := net.ParseIP(config.BindIP)
	if bindIP == nil || bindIP.To4() == nil {
		return Config{}, errors.New("绑定地址必须是 IPv4 地址")
	}
	if len(config.Targets) == 0 {
		config.Targets = []string{"255.255.255.255"}
	}
	normalizedTargets, err := normalizeTargets(config.Targets)
	if err != nil {
		return Config{}, err
	}
	config.Targets = normalizedTargets
	if !config.UDPParty && !config.UDPSetting && !config.UDPAdvertise && !config.TCPParty && !config.TCPSetting && !config.UDPLanInstall && !config.TCPLanInstall {
		return Config{}, errors.New("至少启用一项 UDP 或 TCP 服务")
	}
	return config, nil
}

func normalizeTargets(targets []string) ([]string, error) {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		for _, part := range strings.FieldsFunc(target, func(r rune) bool {
			return r == ',' || r == ';' || r == '\n' || r == '\r'
		}) {
			part = strings.TrimSpace(part)
			ip := net.ParseIP(part)
			if ip == nil || ip.To4() == nil {
				return nil, fmt.Errorf("无效的 IPv4 目标：%s", part)
			}
			canonical := ip.To4().String()
			if _, ok := seen[canonical]; ok {
				continue
			}
			seen[canonical] = struct{}{}
			result = append(result, canonical)
		}
	}
	if len(result) == 0 {
		return nil, errors.New("至少填写一个 UDP 目标地址")
	}
	return result, nil
}

func (a *App) start(config Config) error {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return err
	}

	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return errors.New("测试节点已经启动")
	}

	bindIP := net.ParseIP(normalized.BindIP).To4()
	udpSockets := make(map[ServiceName]*net.UDPConn)
	tcpListeners := make(map[ServiceName]*net.TCPListener)
	cleanup := func() {
		for _, conn := range udpSockets {
			_ = conn.Close()
		}
		for _, listener := range tcpListeners {
			_ = listener.Close()
		}
	}

	for _, service := range []ServiceName{serviceParty, serviceSetting, serviceAdvertise, serviceLanBeacon} {
		if !udpEnabled(normalized, service) {
			continue
		}
		address := &net.UDPAddr{IP: bindIP, Port: servicePorts[service]}
		conn, listenErr := net.ListenUDP("udp4", address)
		if listenErr != nil {
			cleanup()
			a.mu.Unlock()
			return fmt.Errorf("监听 UDP %s (%d) 失败：%w", service, servicePorts[service], listenErr)
		}
		if broadcastErr := enableUDPBroadcast(conn); broadcastErr != nil {
			_ = conn.Close()
			cleanup()
			a.mu.Unlock()
			return fmt.Errorf("启用 UDP 广播 %s (%d) 失败：%w", service, servicePorts[service], broadcastErr)
		}
		udpSockets[service] = conn
	}

	for _, service := range []ServiceName{serviceParty, serviceSetting, serviceLanSync} {
		if !tcpEnabled(normalized, service) {
			continue
		}
		address := &net.TCPAddr{IP: net.IPv4zero, Port: servicePorts[service]}
		listener, listenErr := net.ListenTCP("tcp4", address)
		if listenErr != nil {
			cleanup()
			a.mu.Unlock()
			return fmt.Errorf("监听 TCP %s (%d) 失败：%w", service, servicePorts[service], listenErr)
		}
		tcpListeners[service] = listener
	}

	a.config = normalized
	a.startedAt = time.Now()
	a.udp = udpSockets
	a.tcp = tcpListeners
	a.stopCh = make(chan struct{})
	a.running = true
	a.lanMu.Lock()
	clear(a.lanServers)
	a.lanMu.Unlock()
	a.flowMu.Lock()
	a.phase = "starting"
	a.lanReady = false
	a.settingReady = false
	a.advertiseReady = false
	a.settingStarted = false
	a.lanConnecting = false
	a.settingConnecting = false
	a.settingConnected = false
	a.recruiting = false
	a.recruitID = ""
	a.recruitCancel = nil
	clear(a.invites)
	clear(a.members)
	a.flowMu.Unlock()
	stopCh := a.stopCh
	a.mu.Unlock()

	for service, conn := range udpSockets {
		a.events.add(Event{Level: "info", Transport: "UDP", Service: string(service), Local: conn.LocalAddr().String(), Kind: "LISTEN", Message: "UDP 监听已启动"})
		a.wg.Add(1)
		go a.readUDP(service, conn, normalized, stopCh)
	}
	for service, listener := range tcpListeners {
		a.events.add(Event{Level: "info", Transport: "TCP", Service: string(service), Local: listener.Addr().String(), Kind: "LISTEN", Message: "TCP 监听已启动"})
		a.wg.Add(1)
		go a.acceptTCP(service, listener, normalized)
	}
	a.events.add(Event{Level: "info", Kind: "NODE_STARTED", NodeName: normalized.NodeName, Group: normalized.Group, Message: "测试节点已启动"})
	a.wg.Add(1)
	go a.runStartup(normalized, stopCh)
	return nil
}

func (a *App) stop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return
	}
	udpSockets := a.udp
	tcpListeners := a.tcp
	stopCh := a.stopCh
	a.running = false
	a.udp = make(map[ServiceName]*net.UDPConn)
	a.tcp = make(map[ServiceName]*net.TCPListener)
	a.stopCh = nil
	a.mu.Unlock()
	a.flowMu.Lock()
	if a.recruitCancel != nil {
		close(a.recruitCancel)
		a.recruitCancel = nil
	}
	a.phase = "stopped"
	a.recruiting = false
	a.flowMu.Unlock()
	a.pendingMu.Lock()
	clear(a.pending)
	a.pendingMu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	for _, conn := range udpSockets {
		_ = conn.Close()
	}
	for _, listener := range tcpListeners {
		_ = listener.Close()
	}
	a.wg.Wait()
	a.events.add(Event{Level: "info", Kind: "NODE_STOPPED", Message: "测试节点已停止"})
}

func udpEnabled(config Config, service ServiceName) bool {
	switch service {
	case serviceParty:
		return config.UDPParty && config.Group != "OFF"
	case serviceSetting:
		return config.UDPSetting && config.Group != "OFF"
	case serviceAdvertise:
		return config.UDPAdvertise
	case serviceLanBeacon:
		return config.UDPLanInstall
	default:
		return false
	}
}

func tcpEnabled(config Config, service ServiceName) bool {
	switch service {
	case serviceParty:
		return config.TCPParty && config.Group != "OFF"
	case serviceSetting:
		return config.TCPSetting && config.Group != "OFF" && config.GroupRole == "parent"
	case serviceLanSync:
		return config.TCPLanInstall && config.LANRole == "server"
	default:
		return false
	}
}

func (a *App) setPhase(phase, message string) {
	a.flowMu.Lock()
	a.phase = phase
	a.flowMu.Unlock()
	a.events.add(Event{Level: "info", Kind: "PHASE", Message: message})
}

func (a *App) runStartup(config Config, stopCh <-chan struct{}) {
	defer a.wg.Done()
	a.events.add(Event{Level: "info", Kind: "STARTUP_BEGIN", NodeName: config.NodeName, Group: config.Group, Message: "开始模拟游戏启动前置流程"})
	if !config.UDPLanInstall || !config.TCPLanInstall {
		a.events.add(Event{Level: "warn", Kind: "LAN_INSTALL_SKIPPED", Message: "LAN Install 已关闭，直接进入 Setting 阶段"})
		a.markLANReady(config, stopCh)
		return
	}
	if config.LANRole == "server" {
		a.setPhase("lan_install_server", "LAN Install Server 启动 beacon 与 sync 服务")
		if err := a.startPeriodicUDP(stopCh, serviceLanBeacon, "beacon", 3*time.Second); err != nil {
			a.setStartupError("LAN Install beacon 首包发送失败", err)
			return
		}
		a.markLANReady(config, stopCh)
		return
	}
	a.setPhase("waiting_lan_beacon", "LAN Install Client 等待 Server beacon")
}

func (a *App) markLANReady(config Config, stopCh <-chan struct{}) {
	a.flowMu.Lock()
	if a.lanReady {
		a.flowMu.Unlock()
		return
	}
	a.lanReady = true
	startSetting := !a.settingStarted
	if startSetting {
		a.settingStarted = true
	}
	a.flowMu.Unlock()
	a.events.add(Event{Level: "info", Kind: "LAN_INSTALL_READY", Message: "LAN Install 前置链路已就绪"})
	if !startSetting {
		return
	}
	if config.Group == "OFF" {
		a.events.add(Event{Level: "warn", Kind: "GROUP_OFF", Message: "组别为 OFF，不启动 Setting Parent/Child 与 Party 服务"})
		a.markSettingReady(config)
		return
	}
	if !config.UDPSetting || !config.TCPSetting {
		a.events.add(Event{Level: "warn", Kind: "SETTING_SKIPPED", Message: "Setting UDP/TCP 已关闭，模拟流程直接进入 Ready"})
		a.markSettingReady(config)
		return
	}
	if config.GroupRole == "parent" {
		a.setPhase("setting_parent", "Parent 开始广播 SettingHostAddress")
		if err := a.startPeriodicUDP(stopCh, serviceSetting, "setting_host_address", 3*time.Second); err != nil {
			a.setStartupError("SettingHostAddress 首包发送失败", err)
			return
		}
		a.markSettingReady(config)
		return
	}
	a.setPhase("waiting_setting_host", "Child 等待 Parent 的 SettingHostAddress")
}

func (a *App) markSettingReady(config Config) {
	a.flowMu.Lock()
	if a.settingReady {
		a.flowMu.Unlock()
		return
	}
	a.settingReady = true
	a.phase = "advertise"
	a.flowMu.Unlock()
	a.events.add(Event{Level: "info", Kind: "SETTING_READY", Message: "Setting 前置链路已就绪"})
	if config.UDPAdvertise {
		a.events.add(Event{Level: "info", Kind: "ADVERTISE_BEGIN", Message: "发送 AdvertiseRequest，进入待机广告协调阶段"})
		done := make(chan error, 1)
		if err := a.sendUDP(UDPProbeRequest{Service: string(serviceAdvertise), Kind: "advertise_request", Count: 1, done: done}); err != nil {
			a.recordError("ADVERTISE_SEND_ERROR", "UDP", serviceAdvertise, "", "", err)
			return
		}
		if err := <-done; err != nil {
			a.setStartupError("AdvertiseRequest 发送失败", err)
			return
		}
	} else {
		a.events.add(Event{Level: "warn", Kind: "ADVERTISE_SKIPPED", Message: "Advertise UDP 已关闭，模拟流程直接进入 Ready"})
	}
	a.flowMu.Lock()
	a.advertiseReady = true
	a.phase = "ready"
	a.flowMu.Unlock()
	a.events.add(Event{Level: "info", Kind: "ADVERTISE_READY", Message: "AdvertiseRequest 已提交"})
	a.events.add(Event{Level: "info", Kind: "STARTUP_READY", NodeName: config.NodeName, Group: config.Group, Message: "节点已完成模拟启动，可执行店内招募"})
}

func (a *App) setStartupError(message string, err error) {
	a.flowMu.Lock()
	a.phase = "startup_error"
	a.flowMu.Unlock()
	a.events.add(Event{Level: "error", Kind: "STARTUP_ERROR", Message: message + "：" + err.Error()})
}

func (a *App) startPeriodicUDP(stopCh <-chan struct{}, service ServiceName, kind string, interval time.Duration) error {
	done := make(chan error, 1)
	if err := a.sendUDP(UDPProbeRequest{Service: string(service), Kind: kind, Count: 1, done: done}); err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := a.sendUDP(UDPProbeRequest{Service: string(service), Kind: kind, Count: 1}); err != nil {
					a.recordError("STARTUP_SEND_ERROR", "UDP", service, "", "", err)
				}
			case <-stopCh:
				return
			}
		}
	}()
	return nil
}

func (a *App) readUDP(service ServiceName, conn *net.UDPConn, config Config, stopCh <-chan struct{}) {
	defer a.wg.Done()
	buffer := make([]byte, 65535)
	for {
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-stopCh:
				return
			default:
			}
			a.recordError("UDP_READ_ERROR", "UDP", service, conn.LocalAddr().String(), "", err)
			return
		}
		a.inc("udp_rx")
		payload := append([]byte(nil), buffer[:n]...)
		var message LabMessage
		if err := json.Unmarshal(payload, &message); err != nil || message.Magic != protocolMagic || message.Version != protocolVersion {
			a.events.add(Event{
				Level: "warn", Direction: "RX", Transport: "UDP", Service: string(service),
				Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: "FOREIGN_DATAGRAM", Size: n,
				Message: "收到非 Chusan LAN Lab 报文；前缀=" + hexPrefix(payload, 48),
			})
			continue
		}

		groupMatch := message.Group == config.Group
		event := Event{
			Level: "info", Direction: "RX", Transport: "UDP", Service: string(service),
			Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: strings.ToUpper(message.Type),
			NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch),
			ProbeID: message.ProbeID, Size: n, Message: "收到可识别的实验报文",
		}
		if message.Type == "beacon" {
			event.Message = fmt.Sprintf("收到 LAN Install beacon，角色=%s", message.Role)
		} else if message.Type == "setting_host_address" {
			event.Message = fmt.Sprintf("收到 SettingHostAddress，Host=%s", message.HostIPv4)
		}

		if message.Type == "response" {
			a.pendingMu.Lock()
			started, ok := a.pending[message.ProbeID]
			if ok {
				delete(a.pending, message.ProbeID)
			}
			a.pendingMu.Unlock()
			if ok {
				event.RTTMS = float64(time.Since(started).Microseconds()) / 1000
				event.Message = fmt.Sprintf("收到 UDP 回包，RTT %.3f ms", event.RTTMS)
			}
			a.inc("udp_response_rx")
		}
		a.events.add(event)

		if service == serviceAdvertise {
			if message.InstanceID == a.instanceID {
				continue
			}
			switch message.Type {
			case "advertise_request":
				if groupMatch {
					a.sendUDPDirect(conn, remote, config, serviceAdvertise, "advertise_response", message.ProbeID)
				}
			case "advertise_response":
				if groupMatch {
					a.sendUDPDirect(conn, remote, config, serviceAdvertise, "advertise_go", message.ProbeID)
				}
			case "advertise_go":
				a.events.add(Event{Level: "info", Direction: "RX", Transport: "UDP", Service: string(serviceAdvertise), Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: "ADVERTISE_COMPLETE", NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch), ProbeID: message.ProbeID, Message: "收到 AdvertiseGo，完成 Request/Response/Go 协调"})
			}
			continue
		}

		if service == serviceLanBeacon && message.Type == "beacon" {
			if message.Role == "server" && config.LANRole == "client" {
				a.lanMu.Lock()
				a.lanServers[message.InstanceID] = remote.IP.String()
				serverCount := len(a.lanServers)
				a.lanMu.Unlock()
				if serverCount > 1 {
					a.events.add(Event{
						Level: "warn", Direction: "RX", Transport: "UDP", Service: string(service),
						Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: "MULTIPLE_SERVERS",
						NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch),
						ProbeID: message.ProbeID, Size: n,
						Message: fmt.Sprintf("已发现 %d 个不同 LAN Install Server", serverCount),
					})
				}
				a.flowMu.Lock()
				shouldConnect := !a.lanReady && !a.lanConnecting
				if shouldConnect {
					a.lanConnecting = true
				}
				a.flowMu.Unlock()
				if shouldConnect {
					hostIP := remote.IP.String()
					if advertised := net.ParseIP(message.HostIPv4); advertised != nil && advertised.To4() != nil {
						hostIP = advertised.To4().String()
					}
					go func() {
						result, probeErr := a.probeTCP(TCPProbeRequest{Service: string(serviceLanSync), Target: hostIP, TimeoutMS: 3000})
						a.flowMu.Lock()
						a.lanConnecting = false
						a.flowMu.Unlock()
						if probeErr == nil && result.Responded {
							a.markLANReady(config, stopCh)
						}
					}()
				}
			}
			continue
		}

		if service == serviceSetting && message.Type == "setting_host_address" {
			if config.GroupRole == "child" && groupMatch {
				hostIP := net.ParseIP(message.HostIPv4)
				if hostIP == nil || hostIP.To4() == nil {
					a.recordError("SETTING_HOST_ADDRESS_INVALID", "UDP", service, conn.LocalAddr().String(), remote.String(), fmt.Errorf("载荷中的 Host IPv4 无效：%q", message.HostIPv4))
					continue
				}
				hostIPText := hostIP.To4().String()
				a.events.add(Event{
					Level: "info", Direction: "TX", Transport: "TCP", Service: string(service),
					Remote: net.JoinHostPort(hostIPText, fmt.Sprintf("%d", servicePorts[service])), Kind: "SETTING_AUTO_CONNECT",
					NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch),
					ProbeID: message.ProbeID, Message: "Child 已从 SettingHostAddress 载荷读取 Host 地址，自动发起 TCP 50201",
				})
				a.flowMu.Lock()
				shouldConnect := !a.settingReady && !a.settingConnecting
				if shouldConnect {
					a.settingConnecting = true
				}
				a.flowMu.Unlock()
				if shouldConnect {
					a.wg.Add(1)
					go a.runSettingClient(hostIPText, config, stopCh, message.ProbeID)
				}
			}
			continue
		}

		if service == serviceParty && message.Type == "start_recruit" {
			if message.InstanceID != a.instanceID {
				a.flowMu.Lock()
				_, existed := a.invites[message.ProbeID]
				a.invites[message.ProbeID] = message
				a.flowMu.Unlock()
				a.events.add(Event{
					Level: "info", Direction: "RX", Transport: "UDP", Service: string(service),
					Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: "INVITE_AVAILABLE",
					NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch),
					ProbeID: message.ProbeID,
					Message: fmt.Sprintf("收到店内招募：Host=%s EventMode=%d MusicID=%d", message.HostIPv4, message.EventMode, message.MusicID),
				})
				if config.AutoJoin && !existed {
					go func(invite LabMessage) {
						_, _ = a.joinParty(invite, false)
					}(message)
				}
			}
			continue
		}

		if service == serviceParty && message.Type == "finish_recruit" {
			a.flowMu.Lock()
			delete(a.invites, message.ProbeID)
			a.flowMu.Unlock()
			a.events.add(Event{Level: "info", Direction: "RX", Transport: "UDP", Service: string(service), Kind: "INVITE_FINISHED", ProbeID: message.ProbeID, Message: "Host 已结束店内招募"})
			continue
		}

		if message.Type != "probe" || !config.AutoReply {
			continue
		}
		reply := LabMessage{
			Magic: protocolMagic, Version: protocolVersion, Type: "response",
			InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group,
			Service: string(service), ProbeID: message.ProbeID, TimestampNS: time.Now().UnixNano(),
			EchoNS: message.TimestampNS, GroupMatch: boolPointer(groupMatch), AdvertisedIPs: localIPv4Strings(),
		}
		encoded, err := json.Marshal(reply)
		if err != nil {
			a.recordError("UDP_ENCODE_ERROR", "UDP", service, conn.LocalAddr().String(), remote.String(), err)
			continue
		}
		if _, err := conn.WriteToUDP(encoded, remote); err != nil {
			a.recordError("UDP_REPLY_ERROR", "UDP", service, conn.LocalAddr().String(), remote.String(), err)
			continue
		}
		a.inc("udp_tx")
		a.events.add(Event{
			Level: "info", Direction: "TX", Transport: "UDP", Service: string(service),
			Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: "RESPONSE",
			NodeName: config.NodeName, Group: config.Group, GroupMatch: boolPointer(groupMatch),
			ProbeID: message.ProbeID, Size: len(encoded), Message: "已向探测来源单播回包",
		})
	}
}

func (a *App) sendUDPDirect(conn *net.UDPConn, remote *net.UDPAddr, config Config, service ServiceName, messageType, probeID string) {
	message := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: messageType, InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(service), ProbeID: probeID, TimestampNS: time.Now().UnixNano(), HostIPv4: advertisedHostIPv4(config), EventMode: config.EventMode, MusicID: config.MusicID}
	encoded, err := json.Marshal(message)
	if err != nil {
		a.recordError("UDP_ENCODE_ERROR", "UDP", service, conn.LocalAddr().String(), remote.String(), err)
		return
	}
	if _, err := conn.WriteToUDP(encoded, remote); err != nil {
		a.recordError("UDP_SEND_ERROR", "UDP", service, conn.LocalAddr().String(), remote.String(), err)
		return
	}
	a.inc("udp_tx")
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "UDP", Service: string(service), Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: strings.ToUpper(messageType), NodeName: config.NodeName, Group: config.Group, ProbeID: probeID, Size: len(encoded), Message: "按游戏 Advertise 状态机单播回复"})
}

type UDPProbeRequest struct {
	Service     string   `json:"service"`
	Kind        string   `json:"kind,omitempty"`
	ProbeID     string   `json:"probe_id,omitempty"`
	Targets     []string `json:"targets"`
	Count       int      `json:"count"`
	IntervalMS  int      `json:"interval_ms"`
	PayloadSize int      `json:"payload_size"`
	done        chan error
}

func (a *App) sendUDP(request UDPProbeRequest) error {
	service := ServiceName(strings.ToLower(strings.TrimSpace(request.Service)))
	port, ok := servicePorts[service]
	if !ok {
		return errors.New("未知 UDP 服务")
	}
	if request.Count == 0 {
		request.Count = 1
	}
	if request.Count < 1 || request.Count > 100 {
		return errors.New("发送次数必须为 1～100")
	}
	if request.IntervalMS < 0 || request.IntervalMS > 60000 {
		return errors.New("发送间隔必须为 0～60000 ms")
	}
	if request.PayloadSize < 0 || request.PayloadSize > 900 {
		return errors.New("附加负载必须为 0～900 字节")
	}

	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return errors.New("请先启动测试节点")
	}
	conn := a.udp[service]
	config := a.config
	stopCh := a.stopCh
	a.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("UDP %s 未启用", service)
	}
	if service == serviceLanBeacon && config.LANRole != "server" {
		return errors.New("LAN Install beacon 只能由 LAN Install Server 发送")
	}
	if service == serviceSetting && config.GroupRole != "parent" {
		return errors.New("SettingHostAddress 只能由 Parent 发送")
	}

	targets := request.Targets
	if len(targets) == 0 {
		targets = config.Targets
	}
	normalizedTargets, err := normalizeTargets(targets)
	if err != nil {
		return err
	}
	advertisedIPs := localIPv4Strings()
	hostIPv4 := advertisedHostIPv4(config)
	padding := strings.Repeat("X", request.PayloadSize)

	go func() {
		var completionErr error
		if request.done != nil {
			defer func() {
				request.done <- completionErr
				close(request.done)
			}()
		}
		for iteration := 0; iteration < request.Count; iteration++ {
			for _, target := range normalizedTargets {
				probeID := request.ProbeID
				if probeID == "" {
					probeID = randomID()
				}
				messageType := strings.ToLower(strings.TrimSpace(request.Kind))
				if messageType == "" {
					messageType = "probe"
				}
				if service == serviceLanBeacon && request.Kind == "" {
					messageType = "beacon"
				} else if service == serviceSetting && request.Kind == "" {
					messageType = "setting_host_address"
				}
				role := ""
				if service == serviceLanBeacon {
					role = config.LANRole
				} else if service == serviceSetting {
					role = config.GroupRole
				}
				message := LabMessage{
					Magic: protocolMagic, Version: protocolVersion, Type: messageType,
					InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group,
					Service: string(service), ProbeID: probeID, TimestampNS: time.Now().UnixNano(),
					Role: role, HostIPv4: hostIPv4, EventMode: config.EventMode, MusicID: config.MusicID,
					AdvertisedIPs: advertisedIPs, Padding: padding,
				}
				encoded, marshalErr := json.Marshal(message)
				if marshalErr != nil {
					if completionErr == nil {
						completionErr = marshalErr
					}
					a.recordError("UDP_ENCODE_ERROR", "UDP", service, conn.LocalAddr().String(), target, marshalErr)
					continue
				}
				remote := &net.UDPAddr{IP: net.ParseIP(target).To4(), Port: port}
				if messageType == "probe" {
					a.rememberProbe(probeID)
				}
				if _, writeErr := conn.WriteToUDP(encoded, remote); writeErr != nil {
					if completionErr == nil {
						completionErr = writeErr
					}
					a.pendingMu.Lock()
					delete(a.pending, probeID)
					a.pendingMu.Unlock()
					a.recordError("UDP_SEND_ERROR", "UDP", service, conn.LocalAddr().String(), remote.String(), writeErr)
					continue
				}
				a.inc("udp_tx")
				a.events.add(Event{
					Level: "info", Direction: "TX", Transport: "UDP", Service: string(service),
					Local: conn.LocalAddr().String(), Remote: remote.String(), Kind: strings.ToUpper(messageType),
					NodeName: config.NodeName, Group: config.Group, ProbeID: probeID, Size: len(encoded),
					Message: fmt.Sprintf("发送 %s %d/%d", messageType, iteration+1, request.Count),
				})
			}
			if iteration+1 < request.Count && request.IntervalMS > 0 {
				timer := time.NewTimer(time.Duration(request.IntervalMS) * time.Millisecond)
				select {
				case <-timer.C:
				case <-stopCh:
					timer.Stop()
					return
				}
			}
		}
	}()
	return nil
}

func (a *App) acceptTCP(service ServiceName, listener *net.TCPListener, config Config) {
	defer a.wg.Done()
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			a.recordError("TCP_ACCEPT_ERROR", "TCP", service, listener.Addr().String(), "", err)
			return
		}
		a.inc("tcp_accept")
		a.events.add(Event{
			Level: "info", Direction: "RX", Transport: "TCP", Service: string(service),
			Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(), Kind: "ACCEPT",
			Message: "已接受 TCP 连接",
		})
		a.wg.Add(1)
		go a.handleTCP(service, conn, config)
	}
}

func (a *App) startRecruit() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return errors.New("请先启动节点")
	}
	config := a.config
	stopCh := a.stopCh
	_, udpOK := a.udp[serviceParty]
	_, tcpOK := a.tcp[serviceParty]
	a.mu.Unlock()
	if config.Group == "OFF" {
		return errors.New("组别为 OFF 时不能进行店内招募")
	}
	if !udpOK || !tcpOK {
		return errors.New("店内招募需要同时启用 Party UDP/TCP 50200")
	}

	a.flowMu.Lock()
	if !a.settingReady {
		a.flowMu.Unlock()
		return errors.New("模拟启动流程尚未进入 Ready；请先完成 LAN Install 与 Setting")
	}
	if a.recruiting {
		a.flowMu.Unlock()
		return errors.New("当前已经处于招募状态")
	}
	recruitID := randomID()
	recruitCancel := make(chan struct{})
	a.recruiting = true
	a.recruitID = recruitID
	a.recruitCancel = recruitCancel
	a.phase = "recruiting"
	clear(a.members)
	a.flowMu.Unlock()

	a.events.add(Event{Level: "info", Kind: "RECRUIT_BEGIN", NodeName: config.NodeName, Group: config.Group, ProbeID: recruitID, Message: "开始店内招募；将执行首两帧 51 ms 间隔与后续 6 秒广播"})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_ = a.sendUDP(UDPProbeRequest{Service: string(serviceParty), Kind: "start_recruit", ProbeID: recruitID, Count: 2, IntervalMS: 51})
		timer := time.NewTimer(6 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				_ = a.sendUDP(UDPProbeRequest{Service: string(serviceParty), Kind: "start_recruit", ProbeID: recruitID, Count: 1})
				timer.Reset(6 * time.Second)
			case <-recruitCancel:
				return
			case <-stopCh:
				return
			}
		}
	}()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			invite := LabMessage{ProbeID: recruitID, HostIPv4: "127.0.0.1", Group: config.Group, EventMode: config.EventMode, MusicID: config.MusicID, NodeName: config.NodeName}
			_, _ = a.joinParty(invite, true)
		case <-recruitCancel:
		case <-stopCh:
		}
	}()
	return nil
}

func (a *App) stopRecruit() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return errors.New("节点未启动")
	}
	a.mu.Unlock()

	a.flowMu.Lock()
	if !a.recruiting {
		a.flowMu.Unlock()
		return errors.New("当前没有进行中的招募")
	}
	recruitID := a.recruitID
	if a.recruitCancel != nil {
		close(a.recruitCancel)
	}
	a.recruitCancel = nil
	a.recruiting = false
	a.recruitID = ""
	a.phase = "ready"
	a.flowMu.Unlock()
	_ = a.sendUDP(UDPProbeRequest{Service: string(serviceParty), Kind: "finish_recruit", ProbeID: recruitID, Count: 1})
	a.events.add(Event{Level: "info", Kind: "RECRUIT_FINISH", ProbeID: recruitID, Message: "店内招募已结束"})
	return nil
}

func (a *App) joinInvite(recruitID string) (TCPProbeResult, error) {
	a.flowMu.Lock()
	invite, ok := a.invites[recruitID]
	a.flowMu.Unlock()
	if !ok {
		return TCPProbeResult{}, errors.New("邀请不存在或已经结束")
	}
	return a.joinParty(invite, false)
}

func (a *App) joinParty(invite LabMessage, self bool) (TCPProbeResult, error) {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return TCPProbeResult{}, errors.New("节点未启动")
	}
	config := a.config
	a.mu.Unlock()
	target := invite.HostIPv4
	if self {
		target = "127.0.0.1"
	}
	if ip := net.ParseIP(target); ip == nil || ip.To4() == nil {
		return TCPProbeResult{}, fmt.Errorf("招募携带的 Host IPv4 无效：%q", target)
	}
	address := net.JoinHostPort(target, fmt.Sprintf("%d", servicePorts[serviceParty]))
	started := time.Now()
	kind := "PARTY_CONNECT_BEGIN"
	if self {
		kind = "SELF_JOIN_CONNECT_BEGIN"
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Remote: address, Kind: kind, ProbeID: invite.ProbeID, Message: "开始连接 Party Host"})
	connection, err := a.dialGameLike(serviceParty, target, invite.ProbeID, config)
	if err != nil {
		return TCPProbeResult{Message: err.Error()}, nil
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(8 * time.Second))
	encoder := json.NewEncoder(connection)
	decoder := json.NewDecoder(connection)

	hello := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "hello", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano()}
	if err := encoder.Encode(hello); err != nil {
		return TCPProbeResult{Connected: true, Message: err.Error()}, nil
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Local: connection.LocalAddr().String(), Remote: connection.RemoteAddr().String(), Kind: "HELLO", ProbeID: invite.ProbeID, Message: "发送 Hello"})
	var helloReply LabMessage
	if err := decoder.Decode(&helloReply); err != nil || helloReply.Type != "hello" {
		return TCPProbeResult{Connected: true, Message: "未收到 Host Hello"}, nil
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HELLO", NodeName: helloReply.NodeName, ProbeID: invite.ProbeID, Message: "收到 Host Hello"})

	for i := 0; i < 2; i++ {
		clientState := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "client_state", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(clientState); err != nil {
			return TCPProbeResult{Connected: true, Message: err.Error()}, nil
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "CLIENT_STATE", ProbeID: invite.ProbeID, Message: fmt.Sprintf("发送 ClientState %d/2", i+1)})
	}

	requestJoin := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "request_join", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano(), EventMode: config.EventMode, MusicID: config.MusicID}
	if err := encoder.Encode(requestJoin); err != nil {
		return TCPProbeResult{Connected: true, Message: err.Error()}, nil
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "REQUEST_JOIN", Group: config.Group, ProbeID: invite.ProbeID, Message: fmt.Sprintf("发送 RequestJoin：EventMode=%d MusicID=%d", config.EventMode, config.MusicID)})
	var joinResult LabMessage
	if err := decoder.Decode(&joinResult); err != nil || joinResult.Type != "join_result" {
		return TCPProbeResult{Connected: true, Message: "未收到 JoinResult"}, nil
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "JOIN_RESULT", NodeName: joinResult.NodeName, Group: joinResult.Group, ProbeID: invite.ProbeID, Message: "JoinResult=" + joinResult.Result})
	if joinResult.Result != "Success" {
		return TCPProbeResult{Connected: true, Responded: true, Message: "加入失败：" + joinResult.Result}, nil
	}

	for i := 0; i < 4; i++ {
		var message LabMessage
		if err := decoder.Decode(&message); err != nil {
			return TCPProbeResult{Connected: true, Responded: true, Message: "加入成功，但后续同步中断：" + err.Error()}, nil
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: strings.ToUpper(message.Type), NodeName: message.NodeName, ProbeID: invite.ProbeID, Message: fmt.Sprintf("收到 %s，成员数=%d", message.Type, message.Members)})
	}

	updateUserInfo := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "update_user_info", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano()}
	if err := encoder.Encode(updateUserInfo); err != nil {
		return TCPProbeResult{Connected: true, Responded: true, Message: "成员同步完成，但 UpdateUserInfo 发送失败：" + err.Error()}, nil
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "UPDATE_USER_INFO", ProbeID: invite.ProbeID, Message: "发送 UpdateUserInfo"})

	for i := 0; i < 10; i++ {
		var hostRequest LabMessage
		if err := decoder.Decode(&hostRequest); err != nil || hostRequest.Type != "heart_beat_request" {
			return TCPProbeResult{Connected: true, Responded: true, Message: "等待 Host HeartBeatRequest 失败"}, nil
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_REQUEST", ProbeID: invite.ProbeID, Message: fmt.Sprintf("收到 Host HeartBeatRequest %d/10", i+1)})
		hostResponse := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_response", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(hostResponse); err != nil {
			return TCPProbeResult{Connected: true, Responded: true, Message: "HeartBeatResponse 发送失败：" + err.Error()}, nil
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_RESPONSE", ProbeID: invite.ProbeID, Message: fmt.Sprintf("返回 HeartBeatResponse %d/10", i+1)})

		clientRequest := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_request", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: invite.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(clientRequest); err != nil {
			return TCPProbeResult{Connected: true, Responded: true, Message: "Client HeartBeatRequest 发送失败：" + err.Error()}, nil
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_REQUEST", ProbeID: invite.ProbeID, Message: fmt.Sprintf("发送 Client HeartBeatRequest %d/10", i+1)})
		var clientResponse LabMessage
		if err := decoder.Decode(&clientResponse); err != nil || clientResponse.Type != "heart_beat_response" {
			return TCPProbeResult{Connected: true, Responded: true, Message: "等待 Host HeartBeatResponse 失败"}, nil
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_RESPONSE", ProbeID: invite.ProbeID, Message: fmt.Sprintf("收到 Host HeartBeatResponse %d/10", i+1)})
	}
	if !self {
		a.flowMu.Lock()
		a.phase = "joined"
		delete(a.invites, invite.ProbeID)
		a.flowMu.Unlock()
	}
	rttMS := float64(time.Since(started).Microseconds()) / 1000
	return TCPProbeResult{Connected: true, Responded: true, GroupMatch: boolPointer(true), RTTMS: rttMS, RemoteNode: joinResult.NodeName, Message: "已完成 ClientState、RequestJoin、成员同步、UpdateUserInfo 和双向心跳"}, nil
}

func (a *App) runSettingClient(hostIP string, config Config, stopCh <-chan struct{}, probeID string) {
	defer a.wg.Done()
	connection, err := a.dialGameLike(serviceSetting, hostIP, probeID, config)
	if err != nil {
		a.flowMu.Lock()
		a.settingConnecting = false
		a.flowMu.Unlock()
		return
	}
	defer connection.Close()
	encoder := json.NewEncoder(connection)
	decoder := json.NewDecoder(connection)
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

	request := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "setting_request", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), Role: config.GroupRole, ProbeID: probeID, TimestampNS: time.Now().UnixNano(), EventMode: config.EventMode, MusicID: config.MusicID}
	if err := encoder.Encode(request); err != nil {
		a.recordError("SETTING_REQUEST_SEND_ERROR", "TCP", serviceSetting, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
		return
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Local: connection.LocalAddr().String(), Remote: connection.RemoteAddr().String(), Kind: "SETTING_REQUEST", Group: config.Group, ProbeID: probeID, Message: "发送 SettingRequest"})
	var response LabMessage
	if err := decoder.Decode(&response); err != nil || response.Type != "setting_response" {
		if err == nil {
			err = fmt.Errorf("收到非 SettingResponse：%s", response.Type)
		}
		a.recordError("SETTING_RESPONSE_ERROR", "TCP", serviceSetting, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
		return
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Local: connection.LocalAddr().String(), Remote: connection.RemoteAddr().String(), Kind: "SETTING_RESPONSE", NodeName: response.NodeName, Group: response.Group, ProbeID: probeID, Message: fmt.Sprintf("收到 SettingResponse：EventMode=%d MusicID=%d", response.EventMode, response.MusicID)})
	a.flowMu.Lock()
	a.settingConnecting = false
	a.settingConnected = true
	a.flowMu.Unlock()
	a.markSettingReady(config)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for round := 1; ; round++ {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
		}
		_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
		request := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_request", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), ProbeID: probeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(request); err != nil {
			a.markSettingDisconnected(err)
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_REQUEST", ProbeID: probeID, Message: fmt.Sprintf("Setting Client 心跳 %d", round)})
		var response LabMessage
		if err := decoder.Decode(&response); err != nil || response.Type != "heart_beat_response" {
			if err == nil {
				err = fmt.Errorf("Setting Host 未返回 HeartBeatResponse")
			}
			a.markSettingDisconnected(err)
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_RESPONSE", ProbeID: probeID, Message: fmt.Sprintf("Setting Host 心跳响应 %d", round)})
		var hostRequest LabMessage
		if err := decoder.Decode(&hostRequest); err != nil || hostRequest.Type != "heart_beat_request" {
			if err == nil {
				err = fmt.Errorf("Setting Host 未发送 HeartBeatRequest")
			}
			a.markSettingDisconnected(err)
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_REQUEST", ProbeID: probeID, Message: fmt.Sprintf("收到 Setting Host 心跳 %d", round)})
		reply := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_response", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), ProbeID: probeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(reply); err != nil {
			a.markSettingDisconnected(err)
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_RESPONSE", ProbeID: probeID, Message: fmt.Sprintf("返回 Setting Client 心跳响应 %d", round)})
	}
}

func (a *App) markSettingDisconnected(err error) {
	a.mu.Lock()
	running := a.running
	a.mu.Unlock()
	if !running {
		return
	}
	a.flowMu.Lock()
	a.settingConnected = false
	a.settingReady = false
	a.advertiseReady = false
	a.phase = "setting_disconnected"
	a.flowMu.Unlock()
	a.recordError("SETTING_DISCONNECTED", "TCP", serviceSetting, "", "", err)
}

func (a *App) handleSettingTCP(conn net.Conn, config Config) {
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	var request LabMessage
	if err := decoder.Decode(&request); err != nil || request.Magic != protocolMagic || request.Type != "setting_request" {
		a.events.add(Event{Level: "warn", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "FOREIGN_STREAM", Message: "Setting TCP 已连接，但首包不是 SettingRequest"})
		return
	}
	groupMatch := request.Group == config.Group
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "SETTING_REQUEST", NodeName: request.NodeName, Group: request.Group, GroupMatch: boolPointer(groupMatch), ProbeID: request.ProbeID, Message: "收到 SettingRequest"})
	response := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "setting_response", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), Role: config.GroupRole, ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano(), GroupMatch: boolPointer(groupMatch), EventMode: config.EventMode, MusicID: config.MusicID}
	if err := encoder.Encode(response); err != nil {
		return
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Kind: "SETTING_RESPONSE", Group: config.Group, ProbeID: request.ProbeID, Message: fmt.Sprintf("发送 SettingResponse：EventMode=%d MusicID=%d", config.EventMode, config.MusicID)})

	for round := 1; ; round++ {
		_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
		var clientRequest LabMessage
		if err := decoder.Decode(&clientRequest); err != nil {
			return
		}
		if clientRequest.Type != "heart_beat_request" {
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_REQUEST", NodeName: clientRequest.NodeName, ProbeID: request.ProbeID, Message: fmt.Sprintf("收到 Setting Client 心跳 %d", round)})
		clientResponse := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_response", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(clientResponse); err != nil {
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_RESPONSE", ProbeID: request.ProbeID, Message: fmt.Sprintf("返回 Setting Host 心跳响应 %d", round)})
		hostRequest := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_request", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceSetting), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(hostRequest); err != nil {
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_REQUEST", ProbeID: request.ProbeID, Message: fmt.Sprintf("发送 Setting Host 心跳 %d", round)})
		var hostResponse LabMessage
		if err := decoder.Decode(&hostResponse); err != nil || hostResponse.Type != "heart_beat_response" {
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceSetting), Kind: "HEART_BEAT_RESPONSE", NodeName: hostResponse.NodeName, ProbeID: request.ProbeID, Message: fmt.Sprintf("收到 Setting Client 心跳响应 %d", round)})
	}
}

func (a *App) handlePartyTCP(conn *net.TCPConn, config Config) {
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var hello LabMessage
	if err := decoder.Decode(&hello); err != nil || hello.Magic != protocolMagic || hello.Type != "hello" {
		a.events.add(Event{Level: "warn", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "FOREIGN_STREAM", Message: "Party TCP 已连接，但首包不是 Hello"})
		return
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HELLO", NodeName: hello.NodeName, ProbeID: hello.ProbeID, Message: "收到 Client Hello"})
	helloReply := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "hello", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: hello.ProbeID, TimestampNS: time.Now().UnixNano()}
	if err := encoder.Encode(helloReply); err != nil {
		return
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "HELLO", NodeName: config.NodeName, ProbeID: hello.ProbeID, Message: "返回 Host Hello"})

	var request LabMessage
	for i := 0; i < 3; i++ {
		if err := decoder.Decode(&request); err != nil {
			return
		}
		if request.Type == "client_state" {
			a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "CLIENT_STATE", NodeName: request.NodeName, ProbeID: request.ProbeID, Message: fmt.Sprintf("收到 ClientState %d/2", i+1)})
			continue
		}
		if request.Type == "request_join" {
			break
		}
		return
	}
	if request.Type != "request_join" {
		return
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "REQUEST_JOIN", NodeName: request.NodeName, Group: request.Group, ProbeID: request.ProbeID, Message: fmt.Sprintf("收到 RequestJoin：EventMode=%d MusicID=%d", request.EventMode, request.MusicID)})

	a.flowMu.Lock()
	result := "Success"
	if !a.recruiting || request.ProbeID != a.recruitID {
		result = "NoRecruit"
	} else if request.Group != config.Group {
		result = "DifferentGroup"
	} else if request.EventMode != config.EventMode {
		result = "DifferentEventMode"
	} else if request.MusicID != config.MusicID {
		result = "DifferentMusic"
	} else if len(a.members) >= 4 {
		result = "Full"
	} else if _, exists := a.members[request.InstanceID]; exists {
		result = "AlreadyJoined"
	} else {
		a.members[request.InstanceID] = request.NodeName
	}
	memberCount := len(a.members)
	a.flowMu.Unlock()
	joinResult := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "join_result", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano(), Result: result, Members: memberCount}
	if err := encoder.Encode(joinResult); err != nil {
		return
	}
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "JOIN_RESULT", NodeName: request.NodeName, ProbeID: request.ProbeID, Message: "发送 JoinResult=" + result})
	if result != "Success" {
		return
	}
	for _, messageType := range []string{"party_member_info", "party_member_info", "party_member_state", "party_member_state"} {
		message := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: messageType, InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano(), Members: memberCount}
		if err := encoder.Encode(message); err != nil {
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: strings.ToUpper(messageType), ProbeID: request.ProbeID, Message: fmt.Sprintf("发送 %s，成员数=%d", messageType, memberCount)})
	}
	var updateUserInfo LabMessage
	if err := decoder.Decode(&updateUserInfo); err != nil || updateUserInfo.Type != "update_user_info" {
		return
	}
	a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "UPDATE_USER_INFO", NodeName: updateUserInfo.NodeName, ProbeID: updateUserInfo.ProbeID, Message: "收到 UpdateUserInfo"})

	for i := 0; i < 10; i++ {
		hostRequest := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_request", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(hostRequest); err != nil {
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_REQUEST", ProbeID: request.ProbeID, Message: fmt.Sprintf("发送 Host HeartBeatRequest %d/10", i+1)})
		var hostResponse LabMessage
		if err := decoder.Decode(&hostResponse); err != nil || hostResponse.Type != "heart_beat_response" {
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_RESPONSE", NodeName: hostResponse.NodeName, ProbeID: hostResponse.ProbeID, Message: fmt.Sprintf("收到 Client HeartBeatResponse %d/10", i+1)})

		var clientRequest LabMessage
		if err := decoder.Decode(&clientRequest); err != nil || clientRequest.Type != "heart_beat_request" {
			return
		}
		a.events.add(Event{Level: "info", Direction: "RX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_REQUEST", NodeName: clientRequest.NodeName, ProbeID: clientRequest.ProbeID, Message: fmt.Sprintf("收到 Client HeartBeatRequest %d/10", i+1)})
		clientResponse := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_response", InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group, Service: string(serviceParty), ProbeID: request.ProbeID, TimestampNS: time.Now().UnixNano()}
		if err := encoder.Encode(clientResponse); err != nil {
			return
		}
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(serviceParty), Kind: "HEART_BEAT_RESPONSE", ProbeID: request.ProbeID, Message: fmt.Sprintf("返回 Host HeartBeatResponse %d/10", i+1)})
	}
}

func (a *App) handleTCP(service ServiceName, conn *net.TCPConn, config Config) {
	defer a.wg.Done()
	defer conn.Close()
	if service == serviceParty {
		a.handlePartyTCP(conn, config)
		return
	}
	if service == serviceSetting {
		a.handleSettingTCP(conn, config)
		return
	}
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	reader := bufio.NewReader(io.LimitReader(conn, 65536))
	line, err := reader.ReadString('\n')
	if err != nil {
		a.inc("tcp_handshake_fail")
		a.recordError("TCP_READ_ERROR", "TCP", service, conn.LocalAddr().String(), conn.RemoteAddr().String(), err)
		return
	}
	var message LabMessage
	requestType, responseType := tcpMessageTypes(service)
	if err := json.Unmarshal([]byte(line), &message); err != nil || message.Magic != protocolMagic || message.Version != protocolVersion || message.Type != requestType {
		a.inc("tcp_handshake_fail")
		a.events.add(Event{
			Level: "warn", Direction: "RX", Transport: "TCP", Service: string(service),
			Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(), Kind: "FOREIGN_STREAM",
			Size: len(line), Message: "TCP 已连通，但未收到可识别的实验握手",
		})
		return
	}
	groupMatch := message.Group == config.Group
	a.events.add(Event{
		Level: "info", Direction: "RX", Transport: "TCP", Service: string(service),
		Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(), Kind: strings.ToUpper(requestType),
		NodeName: message.NodeName, Group: message.Group, GroupMatch: boolPointer(groupMatch),
		ProbeID: message.ProbeID, Size: len(line), Message: "收到 TCP 应用层探测",
	})

	reply := LabMessage{
		Magic: protocolMagic, Version: protocolVersion, Type: responseType,
		InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group,
		Service: string(service), ProbeID: message.ProbeID, TimestampNS: time.Now().UnixNano(),
		EchoNS: message.TimestampNS, GroupMatch: boolPointer(groupMatch), AdvertisedIPs: localIPv4Strings(),
	}
	if err := json.NewEncoder(conn).Encode(reply); err != nil {
		a.inc("tcp_handshake_fail")
		a.recordError("TCP_REPLY_ERROR", "TCP", service, conn.LocalAddr().String(), conn.RemoteAddr().String(), err)
		return
	}
	a.inc("tcp_handshake_ok")
	a.events.add(Event{
		Level: "info", Direction: "TX", Transport: "TCP", Service: string(service),
		Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(), Kind: strings.ToUpper(responseType),
		NodeName: config.NodeName, Group: config.Group, GroupMatch: boolPointer(groupMatch),
		ProbeID: message.ProbeID, Message: "已返回 TCP 应用层响应",
	})
}

type TCPProbeRequest struct {
	Service   string `json:"service"`
	Target    string `json:"target"`
	TimeoutMS int    `json:"timeout_ms"`
}

func tcpMessageTypes(service ServiceName) (string, string) {
	switch service {
	case serviceLanSync:
		return "lan_sync_request", "lan_sync_response"
	case serviceSetting:
		return "setting_request", "setting_response"
	case serviceParty:
		return "hello", "hello"
	default:
		return "tcp_probe", "tcp_response"
	}
}

type TCPProbeResult struct {
	Connected  bool    `json:"connected"`
	Responded  bool    `json:"responded"`
	GroupMatch *bool   `json:"group_match,omitempty"`
	RTTMS      float64 `json:"rtt_ms"`
	RemoteNode string  `json:"remote_node,omitempty"`
	Message    string  `json:"message"`
}

type dialResult struct {
	connection net.Conn
	err        error
}

func cabinetFrameDuration(config Config) time.Duration {
	if config.CabinetMode == "SP" {
		return time.Second / 120
	}
	return time.Second / 60
}

func (a *App) dialGameLike(service ServiceName, target, probeID string, config Config) (net.Conn, error) {
	address := net.JoinHostPort(target, fmt.Sprintf("%d", servicePorts[service]))
	dialer := net.Dialer{}
	if config.BindIP != "0.0.0.0" && target != "127.0.0.1" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(config.BindIP).To4()}
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan dialResult, 1)
	started := time.Now()
	a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(service), Remote: address, Kind: "CONNECT_BEGIN", ProbeID: probeID, Message: fmt.Sprintf("按游戏非阻塞状态机开始连接（%s，最多 60 tick）", config.CabinetMode)})
	go func() {
		connection, err := dialer.DialContext(ctx, "tcp4", address)
		resultCh <- dialResult{connection: connection, err: err}
	}()

	select {
	case result := <-resultCh:
		cancel()
		if result.err != nil {
			a.inc("tcp_connect_fail")
			a.recordError("CONNECT_FAILED", "TCP", service, "", address, result.err)
			return nil, result.err
		}
		a.inc("tcp_connect_ok")
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(service), Local: result.connection.LocalAddr().String(), Remote: result.connection.RemoteAddr().String(), Kind: "CONNECT_IMMEDIATE", ProbeID: probeID, RTTMS: float64(time.Since(started).Microseconds()) / 1000, Message: "connect 立即成功"})
		return result.connection, nil
	default:
		a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(service), Remote: address, Kind: "CONNECT_PENDING", ProbeID: probeID, Message: "连接进入 pending，模拟 WSAEWOULDBLOCK/WSAEALREADY 更新路径"})
	}

	ticker := time.NewTicker(cabinetFrameDuration(config))
	defer ticker.Stop()
	for tick := 1; tick <= 60; tick++ {
		select {
		case result := <-resultCh:
			cancel()
			if result.err != nil {
				a.inc("tcp_connect_fail")
				a.recordError("CONNECT_FAILED", "TCP", service, "", address, result.err)
				return nil, result.err
			}
			a.inc("tcp_connect_ok")
			a.events.add(Event{Level: "info", Direction: "TX", Transport: "TCP", Service: string(service), Local: result.connection.LocalAddr().String(), Remote: result.connection.RemoteAddr().String(), Kind: "CONNECT_READY", ProbeID: probeID, RTTMS: float64(time.Since(started).Microseconds()) / 1000, Message: fmt.Sprintf("pending 连接在第 %d/60 tick 完成", tick)})
			return result.connection, nil
		case <-ticker.C:
		}
	}
	cancel()
	err := fmt.Errorf("连接在 60 个 %s 更新周期内未完成", config.CabinetMode)
	a.inc("tcp_connect_fail")
	a.recordError("CONNECT_TIMEOUT", "TCP", service, "", address, err)
	return nil, err
}

func (a *App) probeTCP(request TCPProbeRequest) (TCPProbeResult, error) {
	service := ServiceName(strings.ToLower(strings.TrimSpace(request.Service)))
	_, ok := servicePorts[service]
	if !ok || service == serviceAdvertise {
		return TCPProbeResult{}, errors.New("TCP 仅支持 Party 50200 或 Setting 50201")
	}
	target := strings.TrimSpace(request.Target)
	if target == "" {
		return TCPProbeResult{}, errors.New("请填写目标 IPv4 地址或主机名")
	}
	if request.TimeoutMS == 0 {
		request.TimeoutMS = 3000
	}
	if request.TimeoutMS < 100 || request.TimeoutMS > 30000 {
		return TCPProbeResult{}, errors.New("超时必须为 100～30000 ms")
	}

	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return TCPProbeResult{}, errors.New("请先启动测试节点")
	}
	config := a.config
	a.mu.Unlock()

	probeID := randomID()
	started := time.Now()
	connection, err := a.dialGameLike(service, target, probeID, config)
	if err != nil {
		return TCPProbeResult{Message: err.Error()}, nil
	}
	defer connection.Close()
	connectedMS := float64(time.Since(started).Microseconds()) / 1000

	deadline := time.Now().Add(time.Duration(request.TimeoutMS) * time.Millisecond)
	_ = connection.SetDeadline(deadline)
	requestType, responseType := tcpMessageTypes(service)
	message := LabMessage{
		Magic: protocolMagic, Version: protocolVersion, Type: requestType,
		InstanceID: a.instanceID, NodeName: config.NodeName, Group: config.Group,
		Service: string(service), ProbeID: probeID, TimestampNS: time.Now().UnixNano(), AdvertisedIPs: localIPv4Strings(),
	}
	if service == serviceLanSync {
		message.Role = config.LANRole
	} else if service == serviceSetting {
		message.Role = config.GroupRole
	}
	if err := json.NewEncoder(connection).Encode(message); err != nil {
		a.inc("tcp_handshake_fail")
		a.recordError("TCP_SEND_ERROR", "TCP", service, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
		return TCPProbeResult{Connected: true, RTTMS: connectedMS, Message: "TCP 已连接，但探测发送失败：" + err.Error()}, nil
	}
	var reply LabMessage
	if err := json.NewDecoder(io.LimitReader(connection, 65536)).Decode(&reply); err != nil {
		a.inc("tcp_handshake_fail")
		a.recordError("TCP_RESPONSE_ERROR", "TCP", service, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
		return TCPProbeResult{Connected: true, RTTMS: connectedMS, Message: "TCP 已连接，但未收到应用层回包：" + err.Error()}, nil
	}
	if reply.Magic != protocolMagic || reply.Version != protocolVersion || reply.Type != responseType || reply.ProbeID != probeID {
		a.inc("tcp_handshake_fail")
		return TCPProbeResult{Connected: true, RTTMS: connectedMS, Message: "TCP 已连接，但回包格式或 probe_id 不匹配"}, nil
	}
	rttMS := float64(time.Since(started).Microseconds()) / 1000
	a.inc("tcp_handshake_ok")
	a.events.add(Event{
		Level: "info", Direction: "RX", Transport: "TCP", Service: string(service),
		Local: connection.LocalAddr().String(), Remote: connection.RemoteAddr().String(), Kind: strings.ToUpper(responseType),
		NodeName: reply.NodeName, Group: reply.Group, GroupMatch: reply.GroupMatch,
		ProbeID: probeID, RTTMS: rttMS, Message: fmt.Sprintf("收到 TCP 应用层回包，RTT %.3f ms", rttMS),
	})
	return TCPProbeResult{
		Connected: true, Responded: true, GroupMatch: reply.GroupMatch, RTTMS: rttMS,
		RemoteNode: reply.NodeName, Message: "TCP connect 和双向应用层收发均成功",
	}, nil
}

func (a *App) status() map[string]any {
	a.mu.Lock()
	running := a.running
	config := a.config
	startedAt := a.startedAt
	a.mu.Unlock()
	if config.NodeName == "" {
		config = defaultConfig()
	}
	a.statsMu.Lock()
	stats := make(map[string]uint64, len(a.stats))
	for key, value := range a.stats {
		stats[key] = value
	}
	a.statsMu.Unlock()
	a.flowMu.Lock()
	invites := make([]LabMessage, 0, len(a.invites))
	for _, invite := range a.invites {
		invites = append(invites, invite)
	}
	workflow := map[string]any{
		"phase": a.phase, "lan_ready": a.lanReady, "setting_ready": a.settingReady, "advertise_ready": a.advertiseReady,
		"setting_connected": a.settingConnected, "recruiting": a.recruiting, "recruit_id": a.recruitID, "member_count": len(a.members),
	}
	a.flowMu.Unlock()
	status := map[string]any{
		"running": running, "instance_id": a.instanceID, "config": config,
		"interfaces": interfaceInventory(), "stats": stats, "workflow": workflow, "invites": invites,
	}
	if running {
		status["started_at"] = startedAt.Format(time.RFC3339)
	}
	return status
}

func (a *App) inc(key string) {
	a.statsMu.Lock()
	a.stats[key]++
	a.statsMu.Unlock()
}

func (a *App) rememberProbe(probeID string) {
	started := time.Now()
	a.pendingMu.Lock()
	a.pending[probeID] = started
	a.pendingMu.Unlock()
	time.AfterFunc(60*time.Second, func() {
		a.pendingMu.Lock()
		if current, ok := a.pending[probeID]; ok && current.Equal(started) {
			delete(a.pending, probeID)
		}
		a.pendingMu.Unlock()
	})
}

func (a *App) recordError(kind, transport string, service ServiceName, local, remote string, err error) {
	a.inc("errors")
	a.events.add(Event{
		Level: "error", Transport: transport, Service: string(service), Local: local, Remote: remote,
		Kind: kind, Message: err.Error(),
	})
}

type InterfaceInfo struct {
	Name  string   `json:"name"`
	Index int      `json:"index"`
	MTU   int      `json:"mtu"`
	Flags string   `json:"flags"`
	IPv4  []string `json:"ipv4"`
}

func interfaceInventory() []InterfaceInfo {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	result := make([]InterfaceInfo, 0, len(interfaces))
	for _, iface := range interfaces {
		addresses, _ := iface.Addrs()
		info := InterfaceInfo{Name: iface.Name, Index: iface.Index, MTU: iface.MTU, Flags: iface.Flags.String()}
		for _, address := range addresses {
			var ip net.IP
			switch value := address.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}
			if ip != nil && ip.To4() != nil {
				info.IPv4 = append(info.IPv4, ip.To4().String())
			}
		}
		if len(info.IPv4) > 0 {
			result = append(result, info)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Index < result[j].Index })
	return result
}

func localIPv4Strings() []string {
	var result []string
	for _, iface := range interfaceInventory() {
		result = append(result, iface.IPv4...)
	}
	return result
}

func advertisedHostIPv4(config Config) string {
	if config.BindIP != "" && config.BindIP != "0.0.0.0" {
		if ip := net.ParseIP(config.BindIP).To4(); ip != nil {
			return ip.String()
		}
	}
	for _, iface := range interfaceInventory() {
		for _, address := range iface.IPv4 {
			if ip := net.ParseIP(address).To4(); ip != nil && !ip.IsLoopback() {
				return ip.String()
			}
		}
	}
	return ""
}

func boolPointer(value bool) *bool {
	return &value
}

func randomID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}

func hexPrefix(data []byte, max int) string {
	if len(data) > max {
		data = data[:max]
	}
	return strings.ToUpper(hex.EncodeToString(data))
}

func decodeJSON(request *http.Request, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func methodOnly(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != method {
			writer.Header().Set("Allow", method)
			writeJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		handler(writer, request)
	}
}

func routes(app *App) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", methodOnly(http.MethodGet, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, app.status())
	}))
	mux.HandleFunc("/api/start", methodOnly(http.MethodPost, func(writer http.ResponseWriter, request *http.Request) {
		var config Config
		if err := decodeJSON(request, &config); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := app.start(config); err != nil {
			writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, app.status())
	}))
	mux.HandleFunc("/api/stop", methodOnly(http.MethodPost, func(writer http.ResponseWriter, _ *http.Request) {
		app.stop()
		writeJSON(writer, http.StatusOK, app.status())
	}))
	mux.HandleFunc("/api/recruit/start", methodOnly(http.MethodPost, func(writer http.ResponseWriter, _ *http.Request) {
		if err := app.startRecruit(); err != nil {
			writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, app.status())
	}))
	mux.HandleFunc("/api/recruit/stop", methodOnly(http.MethodPost, func(writer http.ResponseWriter, _ *http.Request) {
		if err := app.stopRecruit(); err != nil {
			writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, app.status())
	}))
	mux.HandleFunc("/api/invite/join", methodOnly(http.MethodPost, func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			RecruitID string `json:"recruit_id"`
		}
		if err := decodeJSON(request, &body); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		result, err := app.joinInvite(strings.TrimSpace(body.RecruitID))
		if err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, result)
	}))
	mux.HandleFunc("/api/udp-probe", methodOnly(http.MethodPost, func(writer http.ResponseWriter, request *http.Request) {
		var probe UDPProbeRequest
		if err := decodeJSON(request, &probe); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := app.sendUDP(probe); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusAccepted, map[string]string{"status": "scheduled"})
	}))
	mux.HandleFunc("/api/tcp-probe", methodOnly(http.MethodPost, func(writer http.ResponseWriter, request *http.Request) {
		var probe TCPProbeRequest
		if err := decodeJSON(request, &probe); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		result, err := app.probeTCP(probe)
		if err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, result)
	}))
	mux.HandleFunc("/api/events/recent", methodOnly(http.MethodGet, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, app.events.snapshot())
	}))
	mux.HandleFunc("/api/events/clear", methodOnly(http.MethodPost, func(writer http.ResponseWriter, _ *http.Request) {
		app.events.clear()
		writeJSON(writer, http.StatusOK, map[string]string{"status": "cleared"})
	}))
	mux.HandleFunc("/api/events/export", methodOnly(http.MethodGet, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=chusan-lan-lab-%s.jsonl", time.Now().Format("20060102-150405")))
		encoder := json.NewEncoder(writer)
		for _, event := range app.events.snapshot() {
			_ = encoder.Encode(event)
		}
	}))
	mux.HandleFunc("/api/events", methodOnly(http.MethodGet, func(writer http.ResponseWriter, request *http.Request) {
		flusher, ok := writer.(http.Flusher)
		if !ok {
			http.Error(writer, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("Cache-Control", "no-cache")
		writer.Header().Set("Connection", "keep-alive")
		_, _ = io.WriteString(writer, ": connected\n\n")
		flusher.Flush()
		channel, unsubscribe := app.events.subscribe()
		defer unsubscribe()
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case event := <-channel:
				encoded, _ := json.Marshal(event)
				fmt.Fprintf(writer, "id: %d\ndata: %s\n\n", event.ID, encoded)
				flusher.Flush()
			case <-ticker.C:
				_, _ = io.WriteString(writer, ": keepalive\n\n")
				flusher.Flush()
			case <-request.Context().Done():
				return
			}
		}
	}))

	webRoot, err := fs.Sub(embeddedWeb, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(webRoot)))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		mux.ServeHTTP(writer, request)
	})
}

func openBrowser(url string) {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		command = exec.Command("open", url)
	default:
		command = exec.Command("xdg-open", url)
	}
	_ = command.Start()
}

func browserURL(listenAddress string) string {
	host, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return "http://127.0.0.1:18080/"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/"
}

func main() {
	listenAddress := flag.String("listen", "127.0.0.1:18080", "GUI HTTP listen address")
	noOpen := flag.Bool("no-open", false, "do not open the browser automatically")
	flag.Parse()

	app := newApp()
	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           routes(app),
		ReadHeaderTimeout: 5 * time.Second,
	}
	contextWithSignal, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	serverError := make(chan error, 1)
	go func() {
		log.Printf("Chusan LAN Lab GUI: %s", browserURL(*listenAddress))
		serverError <- server.ListenAndServe()
	}()
	if !*noOpen {
		time.AfterFunc(350*time.Millisecond, func() { openBrowser(browserURL(*listenAddress)) })
	}

	select {
	case <-contextWithSignal.Done():
	case err := <-serverError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}
	app.stop()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownContext)
}
