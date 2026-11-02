# Runner for the upstream Groovy regression framework: JDK 17 (mandatory for the framework)
# plus Maven. The Doris source tree is mounted at /doris at run time; nothing is baked in.
ARG RUNNER_BASE=maven:3.9-eclipse-temurin-17
FROM ${RUNNER_BASE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash curl git python3 default-mysql-client \
 && rm -rf /var/lib/apt/lists/*
ENV JDK_17=/opt/java/openjdk
WORKDIR /doris
ENTRYPOINT ["/bin/bash", "-lc"]
