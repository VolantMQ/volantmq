// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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

// Package topics deals with MQTT topic names, topic filters and subscriptions.
// - "Topic name" is a / separated string that could contain #, * and $
// - / in topic name separates the string into "topic levels"
// - # is a multi-level wildcard, and it must be the last character in the
//   topic name. It represents the parent and all children levels.
// - + is a single level wildcard. It must be the only character in the
//   topic level. It represents all names in the current level.
// - $ is a special character that says the topic is a system level topic
package topics

import (
	"github.com/VolantMQ/volantmq/topics/memlockfree"
	topicstypes "github.com/VolantMQ/volantmq/topics/types"
)

// New topic provider
func New(config topicstypes.ProviderConfig) (topicstypes.Provider, error) {
	if config == nil {
		return nil, topicstypes.ErrInvalidArgs
	}

	switch cfg := config.(type) {
	case *topicstypes.MemConfig:
		return memlockfree.NewMemProvider(cfg)
	default:
		return nil, topicstypes.ErrUnknownProvider
	}
}
