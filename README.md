SurgeMQ
=======

[![Build Status](https://travis-ci.org/troian/surgemq.svg?branch=master)](https://travis-ci.org/troian/surgemq)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1a43f2f6e0534fd180d0a1b0b8c93614)](https://www.codacy.com/app/troian/surgemq?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=troian/surgemq&amp;utm_campaign=Badge_Grade)
[![codecov.io](https://codecov.io/gh/troian/surgemq/coverage.svg?branch=master)](https://codecov.io/gh/troian/surgemq?branch=master)

This implementation might not be compatible with original.
Original has not been updated since 2014 thus I forked to improve

#### It's currently in active development thus highly not recommended for production

SurgeMQ is a high performance MQTT broker and client library that aims to be fully compliant with MQTT 3.1 and 3.1.1 specs.

**This project should be considered unstable until further notice.**

### MQTT

According to the [MQTT spec](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html):

> MQTT is a Client Server publish/subscribe messaging transport protocol. It is light weight, open, simple, and designed so as to be easy to implement. These characteristics make it ideal for use in many situations, including constrained environments such as for communication in Machine to Machine (M2M) and Internet of Things (IoT) contexts where a small code footprint is required and/or network bandwidth is at a premium.
>
> The protocol runs over TCP/IP, or over other network protocols that provide ordered, lossless, bi-directional connections. Its features include:
> 
> * Use of the publish/subscribe message pattern which provides one-to-many message distribution and decoupling of applications.
> * A messaging transport that is agnostic to the content of the payload.
> * Three qualities of service for message delivery:
>   * "At most once", where messages are delivered according to the best efforts of the operating environment. Message loss can occur. This level could be used, for example, with ambient sensor data where it does not matter if an individual reading is lost as the next one will be published soon after.
>   * "At least once", where messages are assured to arrive but duplicates can occur.
>   * "Exactly once", where message are assured to arrive exactly once. This level could be used, for example, with billing systems where duplicate or lost messages could lead to incorrect charges being applied.
> * A small transport overhead and protocol exchanges minimized to reduce network traffic.
> * A mechanism to notify interested parties when an abnormal disconnection occurs.

There's some very large implementation of MQTT such as [Facebook Messenger](https://www.facebook.com/notes/facebook-engineering/building-facebook-messenger/10150259350998920). There's also an active Eclipse project, [Paho](https://eclipse.org/paho/), that provides scalable open-source client implementations for many different languages, including C/C++, Java, Python, JavaScript, C# .Net and Go.

### Features, Limitations, and Future

**Features**

* Supports QOS 0, 1 and 2 messages
* Supports will messages
* Supports retained messages (add/remove)
* SSL
* Session persistence

**Future**

* $SYS topics
* Server bridge
* Ack timeout/retry

### Performance

TBD

### Compatibility

In addition, SurgeMQ has been tested with the following client libraries and it _seems_ to work:

* libmosquitto 1.3.5 (in C)
  * Tested with the bundled test programs msgsps_pub and msgsps_sub
* Paho MQTT Conformance/Interoperability Testing Suite (in Python)
  * Tested with all 10 test cases. Publish results TBD
* Paho C Client library (in C)
  * Tested with all of the test cases. Publish results TBD

### License

Copyright (c) 2014 Dataence, LLC. All rights reserved.
Copyright (c) 2016 Artur Troian. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


### Examples

Look into examples/surgemq