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

// Package volantmq is a high performance MQTT broker and client library that aims to be
// fully compliant with MQTT 3.1 and 3.1.1 specs.
//
// The primary package that's of interest is package service. It provides the
// MQTT Server and Client services in a library form.
//
//   MQTT is a Client Server publish/subscribe messaging transport protocol. It is
//   light weight, open, simple, and designed so as to be easy to implement. These
//   characteristics make it ideal for use in many situations, including constrained
//   environments such as for communication in Machine to Machine (M2M) and Internet
//   of Things (IoT) contexts where a small code footprint is required and/or network
//   bandwidth is at a premium.
//
//   The protocol runs over TCP/IP, or over other network protocols that provide
//   ordered, lossless, bi-directional connections. Its features include:
//
//   - Use of the publish/subscribe message pattern which provides one-to-many
//     message distribution and decoupling of applications.
//   - A messaging transport that is agnostic to the content of the payload.
//   - Three qualities of service for message delivery:
//   - "At most once", where messages are delivered according to the best efforts
//     of the operating environment. Message loss can occur. This level could be
//     used, for example, with ambient sensor data where it does not matter if an
//     individual reading is lost as the next one will be published soon after.
//     - "At least once", where messages are assured to arrive but duplicates can occur.
//     - "Exactly once", where message are assured to arrive exactly once. This
//       level could be used, for example, with billing systems where duplicate or
//       lost messages could lead to incorrect charges being applied.
//   - A small transport overhead and protocol exchanges minimized to reduce
//     network traffic.
//   - A mechanism to notify interested parties when an abnormal disconnection occurs.
//
// Current performance benchmark of VolantMQ, running all publishers, subscribers
// and broker on a single 4-core (2.8Ghz i7) MacBook Pro, is able to achieve:
//   - over 400,000 MPS in a 1:1 single publisher and single producer configuration
//   - over 450,000 MPS in a 20:1 fan-in configuration
//   - over 750,000 MPS in a 1:20 fan-out configuration
//   - over 700,000 MPS in a full mesh configuration with 20 clients
//
// In addition, VolantMQ has been tested with the following client libraries and
// it _seems_ to work:
//   - libmosquitto 1.3.5 (C)
//     - Tested with the bundled test programs msgsps_pub and msgsps_sub
//   - Paho MQTT Conformance/Interoperability Testing Suite (Python)
//     - Tested with all 10 test cases.
//   - Paho Go Client Library (Go)
//     - Tested with one of the tests in the library, in fact, that tests is now
//       part of the tests for VolantMQ
//   - Paho C Client library (C)
//     - Tested with most of the test cases and failed the same ones as the
//       conformance test because the features are not yet implemented.
//     - Actually I think there's a bug in the test suite as it calls the PUBLISH
//       handler function for non-PUBLISH messages.
package main
