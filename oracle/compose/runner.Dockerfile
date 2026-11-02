# Runner for the upstream Groovy regression framework: JDK 17 (mandatory for the framework),
# Maven, GNU make, and the Thrift 0.24.0 compiler that run-regression-test.sh invokes through
# gensrc/thrift/Makefile. The Doris source tree is mounted at /doris at run time.
ARG RUNNER_BASE=maven:3.9-eclipse-temurin-17

# --- Thrift compiler only (no language libraries). Version must match libthrift in the
# framework pom and upstream thirdparty/vars.sh.
FROM ${RUNNER_BASE} AS thrift-build
ARG THRIFT_VERSION=0.24.0
ARG THRIFT_MD5=232e035ff80c5fb4b7243f0be3a76b02
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential bison flex libboost-dev pkg-config libssl-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -fsSL -o thrift.tar.gz "https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz" \
 && echo "${THRIFT_MD5}  thrift.tar.gz" | md5sum -c - \
 && tar xzf thrift.tar.gz \
 && cd "thrift-${THRIFT_VERSION}" \
 && ./configure --disable-libs --disable-tests --disable-tutorial --without-python --without-java \
 && make -j"$(nproc)" \
 && install -D -m 0755 compiler/cpp/thrift /out/installed/bin/thrift

# --- Final image
FROM ${RUNNER_BASE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash curl git make python3 default-mysql-client \
 && rm -rf /var/lib/apt/lists/*
COPY --from=thrift-build /out/installed/bin/thrift /opt/doris-thirdparty/installed/bin/thrift
ENV JDK_17=/opt/java/openjdk \
    DORIS_THIRDPARTY=/opt/doris-thirdparty \
    PATH=/opt/doris-thirdparty/installed/bin:$PATH
WORKDIR /doris
ENTRYPOINT ["/bin/bash", "-lc"]
