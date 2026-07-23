# AGENTS.MD — samuraipersistor (Clojure) Architecture + House Rules

This repository is a **Clojure microservice** whose responsibility is to persist
speech-to-text transcription events into a **shared Postgres DB**.

It has replaced DB persistence implemented inside individual
transcription workers (e.g. `whisperx_worker`, `finalizer_worker`) in the Python
`xamurai` monorepo.

## Main Rules / Best Practices
- All changes should be documented in the relevant `.md` files. Before claiming
  a task is complete, check the docs for gaps or stale information and fix it.
- For any task, create a dedicated branch (named imperatively) from master (unless told otherwise) and as you progress make small(er) atomic commits named imperatively
- For a task to be completed, there should be no security risks or concerns, always assess task's completion also from the security standpoint and highlight any risks.
- NEVER EXPOSE PORTS on local LAN for testing, always use localhost or 127.0.0.1
- Xamurai used to be called "drsynth", so do not be confused if you do see this name somewhere. That name however is legacy and you should always use xamurai when naming things related to xamurai stack- alternatively, you can also use nanosamurai if the name refers to the whole solution and not just the subset of voice-to-text services
- before you claim any task is complete, run full set of test/integration tests
- Do not read or fetch compiled artifacts when the corresponding source is
  available.
- DO NOT EVER run `clojure -Spath` or similar command to list entire classpath, you would burn too many tokens and rot your context without any significant benefit
- **IMPORTANT:** If you encounter unbalanced parentheses, brackets, or braces in any `.clj`, `.cljs`, `.cljc`, `.cljd`, or EDN-like file: Do NOT try to manually repair them by repeated editing. Run `clj-paren-repair <path>` on the affected file. After repair, re-read the file and continue.
- Avoid oversized namespaces. Break a namespace approaching 1,000 lines into
  smaller logical namespaces.
- Use structured logging for important business operations, including the
  operation's start and successful completion, without logging sensitive data.

## Clojure / ClojureScript editing rules

When editing `.clj`, `.cljs`, `.cljc`, or Hiccup/Reagent code:

1. Never perform large freehand string replacements inside nested Hiccup vectors.
2. Prefer structural tools (MCP / refactor / form-aware tools) over textual patching.
3. For UI changes, rewrite the entire smallest enclosing form (usually the whole `let` binding, helper fn, or entire `[:div ...]` block), not an inline fragment.
4. Before writing code, restate the exact target tree:
    - parent form being changed
    - new siblings to insert
    - whether an existing node is replaced or preserved
5. After edits, always run:
    - clj-kondo on the changed file
    - project formatter if available
    - compile or tests for the touched namespace
6. If lint/compile fails, do not continue feature work. First try `clj-paren-repair` and if that fails try to restore balanced, compiling code.
7. In Hiccup, never insert a sibling form into:
    - a props map
    - a component invocation vector
    - a keyword/value pair sequence
8. When unsure, output the full enclosing form and verify delimiters before applying.

### Clojure Parenthesis Repair
The command `clj-paren-repair` is currently installed on your path.

**IMPORTANT:** You SHOULD NOT try to manually repair parenthesis errors. If you encounter a file with unbalanced parentheses or delimiters, run the `clj-paren-repair` tool on that file instead of attempting to fix the delimiters yourself. IF the tool doesn't work report to the user that they need to fix the delimiter error.

### `clj-paren-repair` Tool Behavior

The `clj-paren-repair` tool exhibits the following behavior:

*   **Delimiter Repair:** It successfully identifies and fixes common delimiter errors, such as unbalanced parentheses.
*   **Code Formatting (`cljfmt`):** The tool automatically formats files using `cljfmt` whenever it processes them, regardless of whether a delimiter error was fixed or not.

## Using clj-kondo (clojure/clojurescript linter)

- Remember that you can run kondo even for clojurescript via `shadow-cljs run clj-kondo.main/main --lint src/cljs`
- For clj linting, you can run it via clojure tools/deps like this: `clj -Sdeps '{:deps {clj-kondo/clj-kondo {:mvn/version "RELEASE"}}}' -M -m clj-kondo.main --lint src/clj`

# About Nanosamur.ai

The Community Edition consists of:

- **SamuraiBFF** (https://github.com/mikub/samuraibff): API, browser UI,
  authentication, and orchestration.
- **xamurai** (https://github.com/nanosamurai/xamurai): speech-to-text and
  speaker-diarization workers.
- **samuraipersistor** (this repo): Kafka-to-Postgres persistence.
- **nanosamurai** (https://github.com/nanosamurai/nanosamurai): the public
  Docker Compose quickstart and operator guidance.
- **nanosamurai-sdk** (https://github.com/nanosamurai/nanosamurai-sdk): the
  Python client SDK.


## 1) Samurai Persistor Service responsibilities

### Inputs
Consumes Kafka topics (protobuf payloads, keyed by `session_id`):

* `transcripts.refined` — `RefinedEvent`
* `transcripts.final` — `SessionTranscript`

### Outputs

* Persists into the shared nanosamurai Postgres tables:
  * `sessions` (assumed to be created by BFF)
  * `recordings`
  * `session_transcripts` (append-only)

* Optionally emits to a DLQ Kafka topic (JSON payload) when persistence cannot
  proceed (e.g. missing session):
  * `transcripts.persistor.dlq`

### Non-goals (for now)

* No query API.
* No auth.
* No schema migrations in this repo.

## 2) Architectural decisions

### Append-only transcript records
We keep `session_transcripts` as an append-only table, following the shared
transcript migration.
This supports multiple transcription worker variants (different models/windows).

### Session ownership
The **BFF owns session creation**. Persistor treats missing sessions as an error
and should send the event to DLQ + commit the Kafka offset (so the consumer group
does not get stuck).

### Kafka ordering / partitioning assumptions
Workers MUST publish messages keyed by `session_id`.
This ensures per-session ordering within a topic.
Cross-topic ordering (`refined` vs `final`) is not guaranteed and not required.

### Backpressure
Kafka poll loop uses a bounded queue. When the queue is full, the consumer pauses
assigned partitions until the queue has space.

### Offset commits
KafkaConsumer is **not thread-safe**.
Offsets are committed **only from the polling thread**. Worker threads enqueue
commit requests.

## 3) Tech stack (locked in)

* Lifecycle: **Integrant**
* Concurrency: threads + bounded queues (simple, predictable)
* Kafka: `org.apache.kafka/kafka-clients`
* Postgres: `next.jdbc` + **HikariCP**
* Logging: `org.corfield.logging4j2` (Log4j2 backend)
* Integration testing: **Testcontainers** (Postgres and later Kafka)
* Protobuf: Java generated stubs vendored in `src/java/samuraibff/proto/*`

## 4) Engineering house rules

### Documentation
* Every public function should have a docstring that states:
  * purpose
  * inputs/outputs (types/shapes)
  * error behavior

### Logging
* Never use `println` for logs.
* Prefer structured logging maps with keys like:
  * `:session-id`, `:tenant-id`
  * `:topic`, `:partition`, `:offset`
  * `:source`, `:type`, `:model`, `:window-length`
* Never log transcript text at INFO by default (security/PII).

### DB
* Use prepared statements.
* Prefer batched inserts where possible.
* Use connection pooling (Hikari).

### Testing
* New persistence logic must have integration tests (Postgres Testcontainers).
* Kafka-related logic must have integration tests (Kafka Testcontainers) before
  we consider it production-ready.

### Security / OWASP-ish defaults
* Config (DB creds, Kafka auth) via environment variables / secrets.
* TLS for Postgres and Kafka in production.
* Health endpoints should be network-restricted at the platform level.
