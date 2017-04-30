// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import "fmt"

// DisconnectMessage The DISCONNECT Packet is the final Control Packet sent from the Client to the Server.
// It indicates that the Client is disconnecting cleanly.
type DisconnectMessage struct {
	header
}

var _ Message = (*DisconnectMessage)(nil)

// NewDisconnectMessage creates a new DISCONNECT message.
func NewDisconnectMessage() *DisconnectMessage {
	msg := &DisconnectMessage{}
	msg.SetType(DISCONNECT) // nolint: errcheck

	return msg
}

// Decode message
func (dm *DisconnectMessage) Decode(src []byte) (int, error) {
	return dm.header.decode(src)
}

// Encode message
func (dm *DisconnectMessage) Encode(dst []byte) (int, error) {
	if !dm.dirty {
		if len(dst) < len(dm.dBuf) {
			return 0, fmt.Errorf("disconnect/Encode: Insufficient buffer size. Expecting %d, got %d", len(dm.dBuf), len(dst))
		}

		return copy(dst, dm.dBuf), nil
	}

	return dm.header.encode(dst)
}
