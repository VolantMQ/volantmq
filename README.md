VolantMQ
=======

[![Build Status](https://travis-ci.org/VolantMQ/volantmq.svg?branch=master)](https://travis-ci.org/volantmq/volantmq)
[![BCH compliance](https://bettercodehub.com/edge/badge/VolantMQ/volantmq?branch=master)](https://bettercodehub.com/)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1a43f2f6e0534fd180d0a1b0b8c93614)](https://www.codacy.com/app/VolantMQ/volantmq?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=VolantMQ/volantmq&amp;utm_campaign=Badge_Grade)
[![codecov.io](https://codecov.io/gh/VolantMQ/volantmq/coverage.svg?branch=master)](https://codecov.io/gh/VolantMQ/volantmq?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

### Warning
**It's currently in active development thus highly not recommended for production**
**This project should be considered unstable until further notice.**

VolantMQ is a high performance MQTT broker that aims to be fully compliant with MQTT 3.1 and 3.1.1 specs .

### Features, Limitations, and Future

**Features**
* [MQTT v3.1 - V3.1.1 compliant](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
* Full support of WebSockets transport
* SSL for both plain tcp and WebSockets transports
* Independent auth providers for each transport
* Persistence providers
* $SYS topics

**Persistence providers**
* [BoltDB](https://github.com/boltdb/bolt)
* In memory

**Future**
* V5.0 specification
* Cluster
* Bridge
### Performance

TBD

### Compatibility

Project has been tested with the following client libraries
* Paho MQTT Conformance/Interoperability Testing Suite (in Python)
  * Pass with all of the test cases
* Paho C Client library (in C)
  * Pass with all of the test cases

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

Look into examples

### [Contributing guidelines](https://github.com/volantmq/volantmq/blob/master/CONTRIBUTING.md)