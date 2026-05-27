// MIT License
//
// Copyright (c) 2016-2017 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package smux

import (
	"io"
	"time"
)

// Config is used to tune the Smux session
type Config struct {
	// SMUX Protocol version, support 1,2
	Version int

	// Disabled keepalive
	KeepAliveDisabled bool

	// KeepAliveInterval is how often to send a NOP command to the remote
	KeepAliveInterval time.Duration

	// KeepAliveTimeout is how long the session
	// will be closed if no data has arrived
	KeepAliveTimeout time.Duration

	// MaxFrameSize is used to control the maximum
	// frame size to sent to the remote
	MaxFrameSize int

	// MaxReceiveBuffer is used to control the maximum
	// number of data in the buffer pool
	MaxReceiveBuffer int

	// MaxStreamBuffer is used to control the maximum
	// number of data per stream
	MaxStreamBuffer int
}

// DefaultConfig is used to return a default configuration
func DefaultConfig() *Config { _ = "STUB: not implemented"; return nil }

// VerifyConfig is used to verify the sanity of configuration
func VerifyConfig(config *Config) error { _ = "STUB: not implemented"; return nil }

// Server is used to initialize a new server-side connection.
func Server(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Client is used to initialize a new client-side connection.
func Client(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
