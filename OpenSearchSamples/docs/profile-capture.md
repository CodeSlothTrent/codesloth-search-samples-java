# Profile API fixture capture

Captures real `_search` responses with `"profile": true` for the CodeSloth **Profile Viewer**.

## Enable and run

```bash
cd OpenSearchSamples
mvn test -Dprofile.enableCapture=true -Dtest='ProfileDemo.*ProfileCaptureTest'
```

Requires Docker. Uses dedicated ports **19300–19302** (does not conflict with SlowLogDemo 19200–19202).

## Outputs

Written under `test-outputs/profile/` as **envelopes**:

```json
{
  "request": { "profile": true, "query": { ... } },
  "response": { "took": ..., "profile": { "shards": [...] } }
}
```

Files:

- `opensearch-2.19.3-search-profile.json`
- `elasticsearch-8.18.0-search-profile.json`
- `elasticsearch-7.10.2-search-profile.json`

Copy into the blog:

```bash
cp test-outputs/profile/*-search-profile.json \
  /Users/trentsky/dev/codesloth-blog/public/tools/profile/
```

The viewer also accepts a bare profile response (no `request`) for waterfall-only use.
