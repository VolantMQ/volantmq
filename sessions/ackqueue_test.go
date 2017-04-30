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

package sessions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
)

func TestAckQueueOutOfOrder(t *testing.T) {
	q := newAckQueue(5)
	require.Equal(t, 8, q.cap())

	for i := 0; i < 12; i++ {
		msg := newPublishMessage(uint16(i), 1)
		q.Wait(msg, nil) // nolint: errcheck
	}

	require.Equal(t, 12, q.len())

	ack1 := message.NewPubAckMessage()
	ack1.SetPacketID(1)
	q.Ack(ack1) // nolint: errcheck

	acked := q.GetAckMsg()

	require.Equal(t, 0, len(acked))

	ack0 := message.NewPubAckMessage()
	ack0.SetPacketID(0)
	q.Ack(ack0) // nolint: errcheck

	acked = q.GetAckMsg()

	require.Equal(t, 2, len(acked))
}
