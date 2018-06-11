FROM golang:1.10.2

ENV \
    VOLANTMQ_WORK_DIR=/var/lib/volantmq \
    VOLANTMQ_BUILD_FLAGS="-i"

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r volantmq && useradd -r -d $VOLANTMQ_WORK_DIR -m -g volantmq volantmq

# Create environment directory
ENV PATH /usr/lib/volantmq/bin:$PATH

RUN mkdir -p $VOLANTMQ_WORK_DIR/{plugins,logs}; \
    mkdir -p /usr/lib/volantmq/bin

# install dep tool
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

# build server
RUN \
    go get github.com/VolantMQ/volantmq && \
    cd $GOPATH/src/github.com/VolantMQ/volantmq && \
    go build $VOLANTMQ_BUILD_FLAGS && \
    cp volantmq /usr/lib/volantmq/bin/

# build debug plugins
RUN \
    cd $GOPATH/src/github.com/VolantMQ/vlapi/plugin/debug && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/debug.so

# build health plugins
RUN \
    cd $GOPATH/src/github.com/VolantMQ/vlapi/plugin/health/plugin && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/health.so

#build persistence plugins
RUN \
    go get github.com/VolantMQ/persistence-boltdb && \
    cd $GOPATH/src/github.com/VolantMQ/persistence-boltdb && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/persistence_boltdb.so

# cleanup
RUN \
    cd / && \
    rm -r $GOPATH/src

RUN chown -R volantmq:volantmq $VOLANTMQ_WORK_DIR

# default config uses mqtt:1883
EXPOSE 1883

CMD ["volantmq"]