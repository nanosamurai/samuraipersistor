# Runtime dependency maintenance

Persistor uses Java 21 and Jackson 2.x. `deps.edn` aligns Jackson core, databind
and Java-time modules at 2.21.8 with annotations 2.21; Jsonista's older transitive
versions must not override these. Log4j API, implementation and bridges are
aligned at 2.25.5. Kafka 4.1.2 uses the maintained `at.yawk.lz4` 1.11.4 codec in
place of `org.lz4`. Keep the old coordinate excluded to avoid duplicate classes.

Generated gRPC classes need the 1.85.0 protocol/stub APIs, but Persistor has no
gRPC client or server. The unused shaded Netty transport is intentionally absent.
Adding actual gRPC networking requires selecting and qualifying a transport;
do not assume the generated protocol classes supply one.

The Docker runtime pins the supported Ubuntu Noble Temurin image by digest.
Until the matching official image is available, its JRE is replaced with the
official Temurin 21.0.13+9 archive, verified against fixed architecture-specific
SHA-256 checksums. CA support, native libraries, timezone handling and UID 10001
remain intact. The selected base has no unused Pebble executable. Replace this
temporary bridge with a scanned immutable official image when available;
updating the base alone must not downgrade the JRE.

These updates do not change application environment variables, database schemas,
topic names or protobuf/JSON message contracts. Health endpoints remain the only
HTTP routes and still need network restrictions.


The bundled default Java truststore follows [Oracle's eMudhra root-removal
advice](https://ops.java/emudhra). The remaining emSign G1 alias is deleted only
after its exact SHA-256 fingerprint matches; the build then checks that all four
listed root subjects are absent. TLS certificates chaining only to these roots
are deliberately rejected. Application-provided truststores and explicit
`javax.net.ssl.trustStore` overrides retain their existing behavior and must be
audited separately; this image change does not sanitize a mounted truststore.

## Release validation

Run `clojure -T:build compile-java` and the full `clojure -X:test` suite with
`TESTCONTAINERS_RYUK_DISABLED=true`. Fixtures publish ports only on localhost and
stop their own containers. The suite includes Postgres persistence, tenant and
track isolation, and an actual Kafka LZ4 roundtrip of protobuf and JSON records.
Format and lint edited Clojure/EDN files, then build the final Docker image.

Record the final image digest, actual `java --version`, dependency-resolution
inventory and a full OS/language vulnerability report with scanner/database
versions. Retain unfixed and lower-severity findings in the report. Flattened
uberjars can hide library metadata from scanners: inspect packaged classes and
shaded version resources too, especially to confirm that Netty and the obsolete
LZ4 implementation are absent. Scan results do not replace runtime/network
hardening or prove that every installed-package advisory is reachable.
