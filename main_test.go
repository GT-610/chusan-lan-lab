package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNormalizeTargets(t *testing.T) {
	targets, err := normalizeTargets([]string{"255.255.255.255, 192.168.139.255", "192.168.139.255"})
	if err != nil {
		t.Fatalf("normalizeTargets returned error: %v", err)
	}
	if got, want := fmt.Sprint(targets), "[255.255.255.255 192.168.139.255]"; got != want {
		t.Fatalf("targets = %s, want %s", got, want)
	}
}

func TestNormalizeConfig(t *testing.T) {
	config := defaultConfig()
	config.Group = "b"
	normalized, err := normalizeConfig(config)
	if err != nil {
		t.Fatalf("normalizeConfig returned error: %v", err)
	}
	if normalized.Group != "B" {
		t.Fatalf("group = %q, want B", normalized.Group)
	}
	if normalized.BindIP != "0.0.0.0" {
		t.Fatalf("bind IP = %q", normalized.BindIP)
	}
}

func TestNormalizeConfigRejectsInvalidGroup(t *testing.T) {
	config := defaultConfig()
	config.Group = "E"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("expected invalid group error")
	}
}

func TestNormalizeConfigRejectsInvalidRoles(t *testing.T) {
	config := defaultConfig()
	config.GroupRole = "host"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("expected invalid group role error")
	}
	config = defaultConfig()
	config.LANRole = "off"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("expected invalid LAN role error")
	}
}

func TestNormalizeConfigCabinetMode(t *testing.T) {
	config := defaultConfig()
	config.CabinetMode = "cvt"
	normalized, err := normalizeConfig(config)
	if err != nil || normalized.CabinetMode != "CVT" {
		t.Fatalf("CVT mode normalization failed: mode=%q err=%v", normalized.CabinetMode, err)
	}
	config.CabinetMode = "arcade"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("expected invalid cabinet mode error")
	}
}

func TestCabinetFrameDuration(t *testing.T) {
	sp := defaultConfig()
	cvt := defaultConfig()
	cvt.CabinetMode = "CVT"
	if cabinetFrameDuration(sp) != time.Second/120 || cabinetFrameDuration(cvt) != time.Second/60 {
		t.Fatal("cabinet frame durations do not match SP/CVT refresh rates")
	}
}

func TestNormalizeConfigGroupOffRules(t *testing.T) {
	config := defaultConfig()
	config.Group = "OFF"
	config.GroupRole = "off"
	if _, err := normalizeConfig(config); err != nil {
		t.Fatalf("OFF group with no cabinet role should be valid: %v", err)
	}
	config.GroupRole = "parent"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("OFF group should reject Parent/Child cabinet role")
	}
	config = defaultConfig()
	config.GroupRole = "off"
	if _, err := normalizeConfig(config); err == nil {
		t.Fatal("A-D group should require Parent/Child cabinet role")
	}
}

func TestCabinetAndLANRolesAreIndependent(t *testing.T) {
	for _, roles := range []struct {
		groupRole string
		lanRole   string
	}{{"parent", "client"}, {"child", "server"}} {
		config := defaultConfig()
		config.GroupRole = roles.groupRole
		config.LANRole = roles.lanRole
		if _, err := normalizeConfig(config); err != nil {
			t.Fatalf("role combination %s/%s should be valid: %v", roles.groupRole, roles.lanRole, err)
		}
	}
}

func TestGroupOffDisablesSettingAndParty(t *testing.T) {
	config := defaultConfig()
	config.Group = "OFF"
	config.GroupRole = "off"
	if udpEnabled(config, serviceParty) || udpEnabled(config, serviceSetting) || tcpEnabled(config, serviceParty) || tcpEnabled(config, serviceSetting) {
		t.Fatal("OFF group should not expose Setting or Party services")
	}
	if !udpEnabled(config, serviceLanBeacon) || !tcpEnabled(config, serviceLanSync) {
		t.Fatal("OFF group should not disable LAN Install services")
	}
}

func TestNormalizeConfigAllowsLANOnly(t *testing.T) {
	config := defaultConfig()
	config.UDPParty = false
	config.UDPSetting = false
	config.UDPAdvertise = false
	config.TCPParty = false
	config.TCPSetting = false
	if _, err := normalizeConfig(config); err != nil {
		t.Fatalf("LAN-only config should be valid: %v", err)
	}
}

func TestRoleSensitiveListeners(t *testing.T) {
	config := defaultConfig()
	if !tcpEnabled(config, serviceSetting) || !tcpEnabled(config, serviceLanSync) {
		t.Fatal("parent/server should expose Setting and LAN Install TCP listeners")
	}
	config.GroupRole = "child"
	config.LANRole = "client"
	if tcpEnabled(config, serviceSetting) || tcpEnabled(config, serviceLanSync) {
		t.Fatal("child/client should not expose host-side TCP listeners")
	}
	if !udpEnabled(config, serviceSetting) || !udpEnabled(config, serviceLanBeacon) {
		t.Fatal("child/client should still receive Setting and LAN Install UDP")
	}
}

func TestTCPMessageTypes(t *testing.T) {
	tests := []struct {
		service  ServiceName
		request  string
		response string
	}{
		{serviceLanSync, "lan_sync_request", "lan_sync_response"},
		{serviceSetting, "setting_request", "setting_response"},
		{serviceParty, "hello", "hello"},
	}
	for _, test := range tests {
		request, response := tcpMessageTypes(test.service)
		if request != test.request || response != test.response {
			t.Fatalf("%s message types = %s/%s, want %s/%s", test.service, request, response, test.request, test.response)
		}
	}
}

func TestSettingRequestResponseAndBidirectionalHeartbeat(t *testing.T) {
	app := newApp()
	config := defaultConfig()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		app.handleSettingTCP(server, config)
	}()
	defer client.Close()
	encoder := json.NewEncoder(client)
	decoder := json.NewDecoder(client)
	probeID := "setting-test"
	request := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "setting_request", InstanceID: "child", NodeName: "child", Group: "A", Service: string(serviceSetting), ProbeID: probeID}
	if err := encoder.Encode(request); err != nil {
		t.Fatal(err)
	}
	var response LabMessage
	if err := decoder.Decode(&response); err != nil || response.Type != "setting_response" {
		t.Fatalf("setting response = %#v err=%v", response, err)
	}
	heartbeat := LabMessage{Magic: protocolMagic, Version: protocolVersion, Type: "heart_beat_request", InstanceID: "child", NodeName: "child", Group: "A", Service: string(serviceSetting), ProbeID: probeID}
	if err := encoder.Encode(heartbeat); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&response); err != nil || response.Type != "heart_beat_response" {
		t.Fatalf("client heartbeat response = %#v err=%v", response, err)
	}
	if err := decoder.Decode(&response); err != nil || response.Type != "heart_beat_request" {
		t.Fatalf("host heartbeat request = %#v err=%v", response, err)
	}
	response.Type = "heart_beat_response"
	response.InstanceID = "child"
	response.NodeName = "child"
	if err := encoder.Encode(response); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	<-done
	kinds := make(map[string]bool)
	for _, event := range app.events.snapshot() {
		kinds[event.Kind] = true
	}
	for _, kind := range []string{"SETTING_REQUEST", "SETTING_RESPONSE", "HEART_BEAT_REQUEST", "HEART_BEAT_RESPONSE"} {
		if !kinds[kind] {
			t.Fatalf("missing setting event %s", kind)
		}
	}
}

func TestEventStoreCap(t *testing.T) {
	store := newEventStore()
	for i := 0; i < maxEvents+25; i++ {
		store.add(Event{Kind: "TEST", Message: fmt.Sprint(i)})
	}
	events := store.snapshot()
	if len(events) != maxEvents {
		t.Fatalf("event count = %d, want %d", len(events), maxEvents)
	}
	if events[0].Message != "25" {
		t.Fatalf("oldest retained event = %q, want 25", events[0].Message)
	}
}

func TestBrowserURL(t *testing.T) {
	if got, want := browserURL("0.0.0.0:18080"), "http://127.0.0.1:18080/"; got != want {
		t.Fatalf("browserURL = %q, want %q", got, want)
	}
}

func TestEmbeddedGUIAndStatusAPI(t *testing.T) {
	server := httptest.NewServer(routes(newApp()))
	defer server.Close()

	response, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "Chusan LAN Lab") {
		t.Fatalf("unexpected GUI response: status=%d", response.StatusCode)
	}

	response, err = http.Get(server.URL + "/api/status")
	if err != nil {
		t.Fatalf("GET /api/status failed: %v", err)
	}
	body, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"running":false`) {
		t.Fatalf("unexpected status response: status=%d body=%s", response.StatusCode, body)
	}
}
