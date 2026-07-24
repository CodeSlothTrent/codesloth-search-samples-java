# Slow log fixture capture

Integration tests in `src/test/java/SlowLogDemo/` spin up dedicated search-engine containers, enable slow logs, run intentionally heavy workloads, and capture real slow-log output for the [CodeSloth slow log viewer](https://codesloth.blog/tools/slow-log-viewer).

These tests are **separate from** the shared `SharedOpenSearchContainer` singleton used by the rest of the demo suite. Each engine binds an alternate host port so capture does not clash with port `9200`.

## Pinned engine images

| Engine | Image tag | Host port |
|--------|-----------|-----------|
| OpenSearch (latest pin) | `opensearchproject/opensearch:2.19.3` | `19200` |
| Elasticsearch (latest pin) | `docker.elastic.co/elasticsearch/elasticsearch:8.18.0` | `19201` |
| Elasticsearch 7.10 | `docker.elastic.co/elasticsearch/elasticsearch:7.10.2` | `19202` |

Versions live in `SlowLogDemo/EngineVersions.java`. Update that file when refreshing fixtures.

## Prerequisites

- Docker running locally (Docker Desktop is fine)
- Java 21 + Maven
- Enough RAM for one engine container at a time (~1 GB heap per container)

### Docker Desktop on macOS notes

Docker Engine **29+** rejects old API versions that older Testcontainers/docker-java builds may negotiate.
The SlowLogDemo helpers set `api.version=1.44` automatically.

If `/var/run/docker.sock` is missing, the helpers also point at `~/.docker/run/docker.sock`.
Alternatively enable **Settings → Advanced → Allow the default Docker socket to be used** in Docker Desktop.

## Run capture tests

Capture tests are **opt-in**. They are disabled during a normal `mvn test` run.

Capture all engines (sequential; each class starts its own container):

```bash
cd OpenSearchSamples
mvn test -Dslowlog.enableCapture=true -Dtest='SlowLogDemo.*SlowLogCaptureTest'
```

Run a single engine:

```bash
# OpenSearch only
mvn test -Dslowlog.enableCapture=true -Dtest='SlowLogDemo.OpenSearchSlowLogCaptureTest'

# Elasticsearch 8.x only
mvn test -Dslowlog.enableCapture=true -Dtest='SlowLogDemo.ElasticsearchLatestSlowLogCaptureTest'

# Elasticsearch 7.10 only
mvn test -Dslowlog.enableCapture=true -Dtest='SlowLogDemo.Elasticsearch710SlowLogCaptureTest'
```

JUnit tags are also available if you prefer tag-based selection:

- `slowlog-capture` — all capture tests
- `opensearch` — OpenSearch capture only
- `elasticsearch-latest` — Elasticsearch 8.x capture only
- `elasticsearch-7.10` — Elasticsearch 7.10 capture only

## Output location

When `-Dslowlog.enableCapture=true` is set, fixtures are written to:

```
OpenSearchSamples/test-outputs/slowlog/
```

Example files:

- `opensearch-2.19.3-search-slowlog.log`
- `opensearch-2.19.3-indexing-slowlog.log`
- `elasticsearch-8.18.0-search-slowlog.log`
- `elasticsearch-8.18.0-indexing-slowlog.log`
- `elasticsearch-7.10.2-search-slowlog.log`
- `elasticsearch-7.10.2-indexing-slowlog.log`

Hand-crafted representative samples (for parser development without Docker) live under:

```
OpenSearchSamples/src/test/resources/slowlog/captured/
```

## Copy fixtures into the blog

After capture, copy the generated `.log` files into the blog static assets:

```bash
cp test-outputs/slowlog/*.log \
  ../../codesloth-blog/public/tools/slow-log/
```

Adjust the destination path if your blog checkout lives elsewhere.

## Workloads

The capture tests deliberately create bottlenecks instead of relying on `0ms` thresholds:

- Leading wildcards on analyzed text (`*lorem*`)
- Expensive Painless script filters
- Large `terms` aggregations and cardinality
- Bulk indexing of documents with large `index: false` payload fields

Official Docker images usually emit slow logs to **container stdout** (not `*slowlog*` files).
The capture helpers read `container.getLogs()` and filter slow-log lines.
