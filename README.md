SurgeMQ
=======

[![Build Status](https://travis-ci.org/troian/surgemq.svg?branch=master)](https://travis-ci.org/troian/surgemq)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1a43f2f6e0534fd180d0a1b0b8c93614)](https://www.codacy.com/app/troian/surgemq?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=troian/surgemq&amp;utm_campaign=Badge_Grade)
[![codecov.io](https://codecov.io/gh/troian/surgemq/coverage.svg?branch=master)](https://codecov.io/gh/troian/surgemq?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

### Warning
This implementation might not be compatible with original.
Original has not been updated since 2014 thus I forked to improve

It's currently in active development thus highly not recommended for production

**This project should be considered unstable until further notice.**

SurgeMQ is a high performance MQTT broker that aims to be fully compliant with MQTT 3.1 and 3.1.1 specs.

### Features, Limitations, and Future

**Features**
* [MQTT v3.1 - V3.1.1 compliant](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
* Full support of WebSockets transport
* SSL for both plain tcp and WebSockets transports
* Independent auth providers for each transport
* Persistence provider by [BoltDB](https://github.com/boltdb/bolt)

**Future**

* $SYS topics
* Cluster
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

Copyright (c) 2016 Artur Troian. All rights reserved.<br/>
Copyright (c) 2014 Dataence, LLC. All rights reserved.

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

### [Contributing guidelines](https://github.com/troian/surgemq/blob/master/CONTRIBUTING.md)