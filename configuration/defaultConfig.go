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
    auth:
      internal:
        type: simpleAuth
        users:
          testuser: testpassword
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
