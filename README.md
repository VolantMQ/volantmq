VolantMQ
=======

[![CircleCI](https://circleci.com/gh/VolantMQ/volantmq/tree/master.svg?style=svg)](https://circleci.com/gh/VolantMQ/volantmq/tree/master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1a43f2f6e0534fd180d0a1b0b8c93614)](https://www.codacy.com/app/VolantMQ/volantmq?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=VolantMQ/volantmq&amp;utm_campaign=Badge_Grade)
[![codecov.io](https://codecov.io/gh/VolantMQ/volantmq/coverage.svg?branch=master)](https://codecov.io/gh/VolantMQ/volantmq?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<a href="https://jetbrains.com/go"> <img src="https://raw.githubusercontent.com/VolantMQ/volantmq/master/3rd-party/logo/jetbrains/logo-text.svg?sanitize=true" width="119"/></a>

![VolantMQ image](doc/images/logo-2.svg)
*VolantMQ image by **Marina Troian**, licensed under [Creative Commons Attribution 4.0 International License][cc-by]

VolantMQ is a high performance MQTT broker that aims to be fully compliant with MQTT specs

##Features
###MQTT Specs
- [MQTT v3.1 - V3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) full support
- [MQTT V5.0](http://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) limited support. see details below
  - [x] Properties
  - [x] Publish expire
  - [x] Session expire
  - [ ] [Shared topics](https://github.com/VolantMQ/volantmq/issues/149)
  - [x] Subscription id
  - [x] Subscription options
  - [ ] [Enhanced authentication](https://github.com/VolantMQ/volantmq/issues/150)
  - [x] Topic alias
  - [x] Server disconnect
  - [x] Flow control
  - [x] Maximum Packet Size
  - [x] Server Keep Alive
  - [x] Assigned ClientID

### Network Transports
- [x] TCP
- [x] TLS
- [x] WebSocket
- [x] WebSocket+TLS

### Persistence
By default server starts with in-memory persistence which means all sessions and messages lost after server restart.

### Plugins
#### Auth
- [x] Server built-in basic auth.Key-value pairs in format `user: sha256 of password` provided by any of the following options
    - Users and their password hashes in config file
    ```yaml
      - name: internal  # authenticator name, used by listeners
        backend: simpleAuth # authenticator type
        config:
          users:
            testuser: "9f735e0df9a1ddc702bf0a1a7b83033f9f7153a00c29de82cedadc9957289b05" # testpassword
    ```
    - Users and their password hashes in separate file
    ```yaml
      - name: internal  # authenticator name, used by listeners
        backend: simpleAuth # authenticator type
        config:
          users: # both can be used simultaneously
            testuser: "9f735e0df9a1ddc702bf0a1a7b83033f9f7153a00c29de82cedadc9957289b05" # testpassword
          usersFile: <some path>
    ```

- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/auth/http/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/auth/http/commits/master) [HTTP](https://gitlab.com/volantmq/vlplugin/auth/http)
#### Monitoring
- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/monitoring/systree/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/monitoring/systree/commits/master) [Systree](https://gitlab.com/volantmq/vlplugin/monitoring/systree)
- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/monitoring/prometheus/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/monitoring/prometheus/commits/master) [Prometheus](https://gitlab.com/volantmq/vlplugin/monitoring/prometheus)
  - [ ] [Grafana dashboard](https://github.com/VolantMQ/volantmq/issues/151)
#### Persistence
- [x] In-Memory server built in
- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/persistence/bbolt/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/persistence/bbolt/commits/master) [BBolt](https://gitlab.com/volantmq/vlplugin/persistence/bbolt)
#### Debug
- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/debug/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/debug/commits/master) [PProf](https://gitlab.com/volantmq/debug/bbolt)
#### Health
- [x] [![Build status](https://gitlab.com/volantmq/vlplugin/health/badges/master/pipeline.svg)](https://gitlab.com/volantmq/vlplugin/health/commits/master) [Health](https://gitlab.com/volantmq/health/bbolt)
## Configuring
Server starts with default config from [here](configuration/defaultConfig.go). Any further configurations applied on top

### Environment variables
- `VOLANTMQ_CONFIG` - path to configuration file described in [this section](#Config file).  
- `VOLANTMQ_PLUGIN_AUTH_HTTP_<NAME>_TOKEN` - API token for auth plugins
  For example to supply auth token into auth plugin `http1` from config below variable should be declared as `VOLANTMQ_PLUGIN_AUTH_HTTP_HTTP1_TOKEN`

### Config file
File divided in a few sections
Complete example can be found [here](examples/config.yaml)

#### System
```yaml
system:
  log:
    console:
      level: info # available levels: debug, info, warn, error, dpanic, panic, fatal
  http:
    defaultPort: 8080 # default HTTP listener assigned. Assigned to plugins like debug/health/metrics if they dont specify own port
```
#### Plugins
```yaml
plugins:
    enabled: # list of plugins server will load on startup
      - systree
      - prometheus
      - debug
      - health
      - auth_http
      - persistence_bbolt
    config: # configuration of each plugin
      <plugin type>:
        - backed: systree # plugin name, allowed: systree, prometheus, http, prof.profiler, health, bbolt
          name: http1     # required by auth plugins only. Value used in auth.order
          config:         # configuration passed to plugin on load stage. refer to particular plugin for configuration
```
#### Default auth config
```yaml
auth:
  anonymous: false # anonymous auth is prohibited. Listener can override
  order: # default auth order. Authenticators invoked in the order they present in the config. Listener can override
    - internal
```
#### MQTT specs
```yaml
mqtt:
  version: // list of supported MQTT specifications
    - v3.1.1
    - v5.0
  keepAlive:
    period: 60 # KeepAlive The number of seconds to keep the connection live if there's no data.
    # Default is 60 seconds
    force: false # Force connection to use server keep alive interval (MQTT 5.0 only)
    # Default is false
  options:
    connectTimeout: 10 # The number of seconds to wait for the CONNACK message before disconnecting.
    # If not set then default to 2 seconds.
    offlineQoS0: true # OfflineQoS0 tell server to either persist (true) or not persist (false) QoS 0 messages for non-clean sessions
    # If not set than default is false
    sessionPreempt: true # AllowDuplicates Either allow or deny replacing of existing session if there new client with same clientID
    # If not set than default is false
    retainAvail: true # don't set to use default
    subsOverlap: false # tells server how to handle overlapping subscriptions from within one client
                       # - true server will send only one publish with max subscribed QoS even there are n subscriptions
                       # - false server will send as many publishes as amount of subscriptions matching publish topic exists
                       # Default is false
    subsId: false # don't set to use default
    subsShared: false # don't set to use default
    subsWildcard: true # don't set to use default
    receiveMax: 65535 # don't set to use default
    maxPacketSize: 268435455 # don't set to use default
    maxTopicAlias: 65535 # don't set to use default
    maxQoS: 2
```
#### Listeners
```yaml
listeners:
  defaultAddr: "0.0.0.0" # default 127.0.0.1
  mqtt: # there are two types of listeners allowed tcp and ws (aka WebSocket) 
    tcp:
      1883:                # port number. can be as many ports configurations as needed
        host: 127.0.0.1    # optional. listen address. defaultAddr is used if omitted
        auth:              # optional. default auth configuration is used if omitted
          anonymous: true  # optional. default auth configuration is used if omitted
          order:           # optional. default auth configuration is used if omitted
            - internal
      1884:
        auth:
          anonymous: false
          order:
            - http1
        tls:               # TLS configuration
          cert:            # path to certificate file
          key:             # path to key file
    ws:
      8883:
        path: mqtt
        auth:
          order:
            - http1
      8884:
        path: mqtt
        auth:
          order:
            - http1
        tls:               # TLS configuration
          cert:            # path to certificate file
          key:             # path to key file
```

Reason to have multiple listeners comes from performance impact of TLS as well as authentication
Internal to system users can omit entire auth and TLS
```text
   ┌──────────────┐                                
   │              │                                
   │ MQTT process │                                
   │              │                                
   └───────▲──────┘                                
           │                                       
           │             ╔════════════════════════╗
           │             ║ VolantMQ               ║
           │             ║                        ║
      ╔════▼═══╗         ║                        ║
      ║intranet◀═════════▶ 1883  # no auth, no TLS║
      ╚════════╝         ║                        ║
      ╔════════╗         ║                        ║
      ║internet◀═════════▶ 1884  # auth and TLS   ║
      ╚═▲══▲══▲╝         ║                        ║
        │  │  │          ╚════════════════════════╝
        │  │  │                                    
   ┌────┘  │  └───┐                                
   │       │      │                                
   │       │      │                                
   │       │      │                                
┌──▼─┐  ┌──▼─┐  ┌─▼──┐                             
│IoT1│  │IoT2│  │IoTn│                             
└────┘  └────┘  └────┘                             
```

## Distribution
- [x] [Docker](https://hub.docker.com/repository/docker/volantmq/volantmq) image contains prebuilt plugins listed in this [section][#Plugins] 
- [ ] [Helm](https://github.com/VolantMQ/volantmq/issues/147)

###How to use
```bash
docker run --rm -p 1883:1883 -p 8080:8080 -v $(pwd)/examples/config.yaml:/etc/volantmq/config.yaml \
--env VOLANTMQ_CONFIG=/etc/volantmq/config.yaml volantmq/volantmq
```

### [Contributing guidelines](https://github.com/volantmq/volantmq/blob/master/CONTRIBUTING.md)

### Credits
Appreciate [JetBrains](https://jetbrains.com) for granted license
