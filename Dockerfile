# syntax=docker/dockerfile:1

ARG TARGETARCH

## --- Builder: compile the persistor uberjar ---
FROM clojure:temurin-21-tools-deps-1.12.6.1673-noble@sha256:fe266a8cd883065e8611e155ac884f58beb61427c3a064c361cd37bdc0fc4767 AS builder-base

FROM builder-base AS builder-amd64
ENV JAVA_VERSION=jdk-21.0.13.1+2

# The affected Base64 intrinsic is x86-only; the ARM64 glibc rebuild is pending.
FROM builder-base AS builder-arm64
ENV JAVA_VERSION=jdk-21.0.13+9

FROM builder-${TARGETARCH} AS builder
ARG TARGETARCH

# Replace the bundled JDK before any Java dependency resolution or compilation.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) jdk_arch=x64; jdk_release='jdk-21.0.13.1%2B2'; jdk_file_version=21.0.13.1_2; jdk_sha256=bb8a93f9ae6e7a693b2b721e85dbeff26ae1a77447985a8d7afcdd9c7cf6222e ;; \
      arm64) jdk_arch=aarch64; jdk_release='jdk-21.0.13%2B9'; jdk_file_version=21.0.13_9; jdk_sha256=e3522e707fc7dbe2a72c30746b1e7068301a0213ecd213d4491f35a14eb55578 ;; \
      *) echo 'Unsupported builder architecture' >&2; exit 1 ;; \
    esac; \
    curl --fail --location --proto '=https' --tlsv1.2 \
      "https://github.com/adoptium/temurin21-binaries/releases/download/${jdk_release}/OpenJDK21U-jdk_${jdk_arch}_linux_hotspot_${jdk_file_version}.tar.gz" \
      --output /tmp/jdk.tar.gz; \
    echo "$jdk_sha256  /tmp/jdk.tar.gz" | sha256sum --check --strict; \
    rm -rf /opt/java/openjdk; \
    mkdir -p /opt/java/openjdk; \
    tar --extract --file /tmp/jdk.tar.gz --directory /opt/java/openjdk \
      --strip-components 1 --no-same-owner; \
    rm /tmp/jdk.tar.gz; \
    java -Xshare:dump; \
    java --version

WORKDIR /app

# The builder JDK downloads dependencies over TLS; refuse eMudhra roots removed
# by Temurin 21.0.13+9. https://ops.java/emudhra
RUN set -eux; \
    keytool -list -v -cacerts -storepass changeit > /tmp/builder-cacerts.txt; \
    if grep -E 'Owner: CN=(emSign (ECC Root CA - G3|Root CA - G2)|AAA Certificate Services),' /tmp/builder-cacerts.txt; then \
      echo 'Disallowed root remains in the builder Java truststore' >&2; exit 1; \
    fi; \
    rm /tmp/builder-cacerts.txt

COPY deps.edn build.clj ./
RUN clojure -P

COPY resources ./resources
COPY src ./src

RUN clojure -T:build uber

## --- Runtime: supported Ubuntu LTS with a verified Temurin JRE ---
FROM eclipse-temurin:21-jre-noble@sha256:35501d3f93173b420ac9001658eb68157e813316b865087e345c187fca422c4c AS runtime-base

FROM runtime-base AS runtime-amd64
ENV JAVA_VERSION=jdk-21.0.13.1+2

FROM runtime-base AS runtime-arm64
ENV JAVA_VERSION=jdk-21.0.13+9

FROM runtime-${TARGETARCH} AS runtime

# Keep the runtime current while the corresponding official container is pending.
# Checksums come from the Adoptium release metadata, not from the download itself.
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) jre_arch=x64; jre_release='jdk-21.0.13.1%2B2'; jre_file_version=21.0.13.1_2; jre_sha256=18f44fab9065217f3ba61d0eef8a7b5f3c3713aa7293001b0d4ac10ede3850f0 ;; \
      arm64) jre_arch=aarch64; jre_release='jdk-21.0.13%2B9'; jre_file_version=21.0.13_9; jre_sha256=b1734db7ade49c3f6acc6ae06233b8c2dd90e3e2f00f67000ae1c972aaa64d8c ;; \
      *) echo "Unsupported runtime architecture" >&2; exit 1 ;; \
    esac; \
    curl --fail --location --proto '=https' --tlsv1.2 \
      "https://github.com/adoptium/temurin21-binaries/releases/download/${jre_release}/OpenJDK21U-jre_${jre_arch}_linux_hotspot_${jre_file_version}.tar.gz" \
      --output /tmp/jre.tar.gz; \
    echo "$jre_sha256  /tmp/jre.tar.gz" | sha256sum --check --strict; \
    rm -rf /opt/java/openjdk; \
    mkdir -p /opt/java/openjdk; \
    tar --extract --file /tmp/jre.tar.gz --directory /opt/java/openjdk \
      --strip-components 1 --no-same-owner; \
    rm /tmp/jre.tar.gz; \
    java -Xshare:dump; \
    java --version

# Follow Oracle's eMudhra root-removal guidance for the bundled default store.
# https://ops.java/emudhra -- custom application truststores need separate review.
RUN set -eux; \
    alias='cn_emsign_root_ca__g1,o_emudhra_technologies_limited,ou_emsign_pki,c_in [jdk]'; \
    if keytool -list -v -cacerts -storepass changeit -alias "$alias" > /tmp/emsign-g1.txt; then \
      grep -F 'SHA256: 40:F6:AF:03:46:A9:9A:A1:CD:1D:55:5A:4E:9C:CE:62:C7:F9:63:46:03:EE:40:66:15:83:3D:C8:C8:D0:03:67' /tmp/emsign-g1.txt; \
      keytool -delete -cacerts -storepass changeit -alias "$alias"; \
    fi; \
    rm /tmp/emsign-g1.txt; \
    keytool -list -v -cacerts -storepass changeit > /tmp/default-cacerts.txt; \
    if grep -E 'Owner: CN=(emSign (ECC Root CA - G3|Root CA - G[12])|AAA Certificate Services),' /tmp/default-cacerts.txt; then \
      echo 'Disallowed root remains in the default Java truststore' >&2; exit 1; \
    fi; \
    rm /tmp/default-cacerts.txt

WORKDIR /app

RUN useradd -r -u 10001 -g root samuraipersistor

COPY --from=builder /app/target/samuraipersistor.jar /app/samuraipersistor.jar

EXPOSE 8010

ENV JAVA_TOOL_OPTIONS="-XX:-UseBASE64Intrinsics"

USER 10001

ENTRYPOINT ["java", "-jar", "/app/samuraipersistor.jar"]
