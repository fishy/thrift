/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"net"
	"sync/atomic"
	"time"
)

// Because net.Listener is an interface, using it directly in atomic.Value poses
// the risk of the concrete type changes, which would cause panic when calling
// atomic.Value.Store. Using a wrapper type to make sure that the concrete type
// stored inside atomic.Value never changes.
//
// This also makes storing nil listener possible.
type listenerWrapper struct {
	l net.Listener
}

type TServerSocket struct {
	addr          net.Addr
	clientTimeout time.Duration

	// Protected by only using atomic read/write.
	listener    atomic.Value // actual type: *listenerWrapper
	interrupted int32
}

func NewTServerSocket(listenAddr string) (*TServerSocket, error) {
	return NewTServerSocketTimeout(listenAddr, 0)
}

func NewTServerSocketTimeout(listenAddr string, clientTimeout time.Duration) (*TServerSocket, error) {
	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &TServerSocket{addr: addr, clientTimeout: clientTimeout}, nil
}

// Creates a TServerSocket from a net.Addr
func NewTServerSocketFromAddrTimeout(addr net.Addr, clientTimeout time.Duration) *TServerSocket {
	return &TServerSocket{addr: addr, clientTimeout: clientTimeout}
}

func (p *TServerSocket) setListener(l net.Listener) {
	p.listener.Store(&listenerWrapper{l: l})
}

func (p *TServerSocket) getListener() net.Listener {
	if w, ok := p.listener.Load().(*listenerWrapper); ok && w != nil {
		return w.l
	}
	return nil
}

func (p *TServerSocket) Listen() error {
	if p.IsListening() {
		return nil
	}
	l, err := net.Listen(p.addr.Network(), p.addr.String())
	if err != nil {
		return err
	}
	p.setListener(l)
	return nil
}

func (p *TServerSocket) Accept() (TTransport, error) {
	if atomic.LoadInt32(&p.interrupted) != 0 {
		return nil, errTransportInterrupted
	}

	listener := p.getListener()
	if listener == nil {
		return nil, NewTTransportException(NOT_OPEN, "No underlying server socket")
	}

	conn, err := listener.Accept()
	if err != nil {
		return nil, NewTTransportExceptionFromError(err)
	}
	return NewTSocketFromConnTimeout(conn, p.clientTimeout), nil
}

// Checks whether the socket is listening.
func (p *TServerSocket) IsListening() bool {
	return p.getListener() != nil
}

// Connects the socket, creating a new socket object if necessary.
func (p *TServerSocket) Open() error {
	if p.IsListening() {
		return NewTTransportException(ALREADY_OPEN, "Server socket already open")
	}
	if l, err := net.Listen(p.addr.Network(), p.addr.String()); err != nil {
		return err
	} else {
		p.setListener(l)
	}
	return nil
}

func (p *TServerSocket) Addr() net.Addr {
	if l := p.getListener(); l != nil {
		return l.Addr()
	}
	return p.addr
}

func (p *TServerSocket) Close() error {
	var err error
	if l := p.getListener(); l != nil {
		err = l.Close()
		p.setListener(nil)
	}
	return err
}

func (p *TServerSocket) Interrupt() error {
	atomic.StoreInt32(&p.interrupted, 1)
	p.Close()

	return nil
}
