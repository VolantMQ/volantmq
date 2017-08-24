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

package packet

// PingResp A PINGRESP Packet is sent by the Server to the Client in response to a PINGREQ
// Packet. It indicates that the Server is alive.
type PingResp struct {
	header
}

var _ Provider = (*PingResp)(nil)

func newPingResp() Provider {
	return &PingResp{}
}

// decode message
func (msg *PingResp) decodeMessage(src []byte) (int, error) {
	return 0, nil
}

func (msg *PingResp) encodeMessage(dst []byte) (int, error) {
	return 0, nil
}

// Encode message
//func (msg *PingResp) Encode(dst []byte) (int, error) {
//	expectedSize, err := msg.Size()
//	if err != nil {
//		return 0, err
//	}
//
//	if len(dst) < expectedSize {
//		return 0, ErrInsufficientBufferSize
//	}
//
//	return msg.preEncode(dst), nil
//}

// Len of message
func (msg *PingResp) size() int {
	return 0
}
