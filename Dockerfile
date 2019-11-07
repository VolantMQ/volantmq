FROM golang:1.13.4 as builder
LABEL stage=intermediate

#compile linux only
ENV \
    GOOS=linux \
    VOLANTMQ_WORK_DIR=/usr/lib/volantmq \
    VOLANTMQ_BUILD_FLAGS="-i" \
    VOLANTMQ_PLUGINS_DIR=/usr/lib/volantmq/plugins \
    GO111MODULE=on

RUN mkdir -p $VOLANTMQ_WORK_DIR/bin
RUN mkdir -p $VOLANTMQ_WORK_DIR/conf
RUN mkdir -p $VOLANTMQ_PLUGINS_DIR

# Create environment directory
ENV PATH $VOLANTMQ_WORK_DIR/bin:$PATH

# build server
RUN \
       GO111MODULE=off go get -v github.com/VolantMQ/volantmq/cmd/volantmq \
    && cd $GOPATH/src/github.com/VolantMQ/volantmq \
    && GO111MODULE=on go mod tidy \
    && go build $VOLANTMQ_BUILD_FLAGS -o $VOLANTMQ_WORK_DIR/bin/volantmq

# build debug plugins
RUN \
       GO111MODULE=off go get gitlab.com/VolantMQ/vlplugin/debug \
    && cd $GOPATH/src/gitlab.com/VolantMQ/vlplugin/debug \
    && GO111MODULE=on go mod tidy \
    && go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/debug.so

# build health plugins
RUN \
       GO111MODULE=off go get gitlab.com/VolantMQ/vlplugin/health \
    && cd $GOPATH/src/gitlab.com/VolantMQ/vlplugin/health \
    && GO111MODULE=on go mod tidy \
    && go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/health.so

#build persistence plugins
RUN \
       GO111MODULE=off go get gitlab.com/VolantMQ/vlplugin/persistence/bbolt \
    && cd $GOPATH/src/gitlab.com/VolantMQ/vlplugin/persistence/bbolt \
    && GO111MODULE=on go mod tidy \
    && cd plugin \
    && go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/persistence_bbolt.so

FROM ubuntu
ENV \
    VOLANTMQ_WORK_DIR=/usr/lib/volantmq

COPY --from=builder $VOLANTMQ_WORK_DIR $VOLANTMQ_WORK_DIR

# Create environment directory
ENV PATH $VOLANTMQ_WORK_DIR/bin:$PATH
ENV VOLANTMQ_PLUGINS_DIR=$VOLANTMQ_WORK_DIR/plugins

# default config uses mqtt:1883
EXPOSE 1883
CMD ["volantmq"]
