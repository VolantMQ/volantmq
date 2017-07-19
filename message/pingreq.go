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

// PingReqMessage The PINGREQ Packet is sent from a Client to the Server. It can be used to:
// 1. Indicate to the Server that the Client is alive in the absence of any other
//    Control Packets being sent from the Client to the Server.
// 2. Request that the Server responds to confirm that it is alive.
// 3. Exercise the network to indicate that the Network Connection is active.
type PingReqMessage struct {
	header
}

var _ Provider = (*PingReqMessage)(nil)

func newPingReqMessage() *PingReqMessage {
	return &PingReqMessage{}
}

// decode message
func (msg *PingReqMessage) decodeMessage(src []byte) (int, error) {
	return 0, nil
}

func (msg *PingReqMessage) encodeMessage(dst []byte) (int, error) {
	return 0, nil
}

// Encode message
//func (msg *PingReqMessage) Encode(dst []byte) (int, error) {
//	expectedSize, err := msg.Size()
//	if err != nil {
//		return 0, err
//	}
//
//	if len(dst) < expectedSize {
//		return expectedSize, ErrInsufficientBufferSize
//	}
//
//	return msg.preEncode(dst), nil
//}

// Len of message
func (msg *PingReqMessage) size() int {
	return 0
}
