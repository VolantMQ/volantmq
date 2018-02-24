FROM golang:1.10.0

ENV \
    VOLANTMQ_WORK_DIR=/var/lib/volantmq \
    VOLANTMQ_BUILD_FLAGS=""

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r volantmq && useradd -r -d ${VOLANTMQ_WORK_DIR} -m -g volantmq volantmq

# Create environment directory
ENV PATH /usr/lib/rabbitmq/bin:$PATH

RUN mkdir ${VOLANTMQ_WORK_DIR}/{plugins,logs}

# default config uses mqtt:1883
EXPOSE 1883

CMD ["volantmq"]