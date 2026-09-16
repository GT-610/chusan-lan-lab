//go:build windows

package main

import (
	"net"
	"syscall"
)

const (
	windowsSOLSocket   = 0xffff
	windowsSOBroadcast = 0x20
)

func enableUDPBroadcast(conn *net.UDPConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		socketErr = syscall.SetsockoptInt(syscall.Handle(fd), windowsSOLSocket, windowsSOBroadcast, 1)
	}); err != nil {
		return err
	}
	return socketErr
}
