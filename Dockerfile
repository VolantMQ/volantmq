FROM golang:1.10.0

ENV \
    VOLANTMQ_WORK_DIR=/var/lib/volantmq \
    VOLANTMQ_BUILD_FLAGS=""

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r volantmq && useradd -r -d ${VOLANTMQ_WORK_DIR} -m -g volantmq volantmq

# Create environment directory
ENV PATH /usr/lib/rabbitmq/bin:$PATH

RUN mkdir -p ${VOLANTMQ_WORK_DIR}/{plugins,logs}; \
    mkdir -p /usr/lib/rabbitmq/bin

RUN go get github.com/VolantMQ/volantmq && \
    curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh && \
    cd $GOPATH/src/github.com/VolantMQ/volantmq && \
    dep ensure && \
    go build && \
    cp volantmq /usr/lib/rabbitmq/bin/ && \
    cd / && \
    rm -r $GOPATH/src

RUN chown -R volantmq:volantmq ${VOLANTMQ_WORK_DIR}

# default config uses mqtt:1883
EXPOSE 1883

CMD ["volantmq"]