package configuration

// defaultConfig loaded anyway when server starts
// may be extended/replaced by user-provided config later
var defaultConfig = []byte(`
version: v0.0.1
system:
  log:
    console:
      config:
        level: info # available levels: debug, info, warn, error, dpanic, panic, fatal
plugins:
  config:
    auth: # plugin type
      - name: internal
        backend: simpleAuth
        config:
          users:
            testuser: "e0d7d338cb1259086d775c964fba50b2a84244ba4cd2815e9f6f4a8d9daaa656" # password must be sha-256 hashed
auth:
  defaultOrder:
    - internal
mqtt:
  version:
  - v3.1.1
  keepAlive:
    period: 60
    force: false
  systree:
    enabled: true
    updateInterval: 10
  options:
    connectTimeout: 2
    offlineQoS0: true
    sessionDups: false
    retainAvail: true
    subsOverlap: false
    subsId: false
    subsShared: false
    subsWildcard: true
    receiveMax: 65535
    maxPacketSize: 268435455
    maxTopicAlias: 65535
    maxQoS: 2
listeners:
  defaultAddr: ""
  mqtt:
    tcp:
      1883:
        def: ""
`)
