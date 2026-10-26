# syntax=docker/dockerfile:1

FROM clojure:temurin-21-tools-deps AS builder

WORKDIR /app

COPY deps.edn build.clj ./
RUN clojure -P

COPY resources ./resources
COPY src ./src

RUN clojure -T:build uber

## --- Runtime: supported Ubuntu LTS with a verified Temurin JRE ---
FROM eclipse-temurin:21-jre-noble@sha256:35501d3f93173b420ac9001658eb68157e813316b865087e345c187fca422c4c AS runtime

# Keep the runtime current while the corresponding official container is pending.
# Checksums come from the Adoptium release metadata, not from the download itself.
ARG TARGETARCH
ENV JAVA_VERSION=jdk-21.0.13+9
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) jre_arch=x64; jre_sha256=a535a1df9c6284d9618872984e026ae0352d1f7e9e0605425c0e6114f47351dd ;; \
      arm64) jre_arch=aarch64; jre_sha256=b1734db7ade49c3f6acc6ae06233b8c2dd90e3e2f00f67000ae1c972aaa64d8c ;; \
      *) echo "Unsupported runtime architecture" >&2; exit 1 ;; \
    esac; \
    curl --fail --location --proto '=https' --tlsv1.2 \
      "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jre_${jre_arch}_linux_hotspot_21.0.13_9.tar.gz" \
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
    keytool -list -v -cacerts -storepass changeit -alias "$alias" \
      | grep -F 'SHA256: 40:F6:AF:03:46:A9:9A:A1:CD:1D:55:5A:4E:9C:CE:62:C7:F9:63:46:03:EE:40:66:15:83:3D:C8:C8:D0:03:67'; \
    keytool -delete -cacerts -storepass changeit -alias "$alias"; \
    keytool -list -v -cacerts -storepass changeit > /tmp/default-cacerts.txt; \
    if grep -E 'Owner: CN=(emSign (ECC Root CA - G3|Root CA - G[12])|AAA Certificate Services),' /tmp/default-cacerts.txt; then \
      echo 'Disallowed root remains in the default Java truststore' >&2; exit 1; \
    fi; \
    rm /tmp/default-cacerts.txt

WORKDIR /app

RUN useradd -r -u 10001 -g root samuraipersistor

COPY --from=builder /app/target/samuraipersistor.jar /app/samuraipersistor.jar

EXPOSE 8010

USER 10001

ENTRYPOINT ["java", "-jar", "/app/samuraipersistor.jar"]
