VolantMQ
=======

[![Build Status](https://travis-ci.org/VolantMQ/volantmq.svg?branch=master)](https://travis-ci.org/VolantMQ/volantmq)
[![BCH compliance](https://bettercodehub.com/edge/badge/VolantMQ/volantmq?branch=master)](https://bettercodehub.com/)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1a43f2f6e0534fd180d0a1b0b8c93614)](https://www.codacy.com/app/VolantMQ/volantmq?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=VolantMQ/volantmq&amp;utm_campaign=Badge_Grade)
[![codecov.io](https://codecov.io/gh/VolantMQ/volantmq/coverage.svg?branch=master)](https://codecov.io/gh/VolantMQ/volantmq?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<a href="https://jetbrains.com/go"> <img src="https://raw.githubusercontent.com/VolantMQ/volantmq/master/3rd-party/logo/jetbrains/logo-text.svg?sanitize=true" width="120"/></a>

VolantMQ is a high performance MQTT broker that aims to be fully compliant with MQTT specs

### Features, Limitations, and Future

**Features**
* [MQTT v3.1 - V3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
* [MQTT V5.0](http://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
* WebSockets transport
* SSL for both plain tcp and WebSockets transports
* Independent auth providers for each transport
* Persistence providers

**Persistence providers**
* [BBolt](https://github.com/coreos/bbolt)
* In memory

### Compatibility

Project has been tested with the following client libraries
* Paho MQTT Conformance/Interoperability Testing Suite (in Python)
  * Pass with all of the test cases
* Paho C Client library (in C)
  * Pass with all of the test cases

### How to use
Best option is to run prebuilt docker image
```bash
docker run --rm -p 1883:1883 -p 8080:8080 -v $(pwd)/examples/config.yaml:/etc/volantmq/config.yaml --env VOLANTMQ_CONFIG=/etc/volantmq/config.yaml volantmq/volantmq
```

- In example above port 1883 is mqtt listener with default user/password testuser/testpassword
- Port 8080 exposes healthcheck endpoints as well as pprof at http://localhost:8080/debug/pprof

#### 
### [Contributing guidelines](https://github.com/volantmq/volantmq/blob/master/CONTRIBUTING.md)

### Credits
Appreciate [JetBrains](https://jetbrains.com) for granted license
