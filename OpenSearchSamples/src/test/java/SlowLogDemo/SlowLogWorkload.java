package SlowLogDemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Intentionally expensive indexing and search workloads so captured slow logs show real bottlenecks.
 */
public final class SlowLogWorkload {

    public static final String INDEX_NAME = "slowlog-demo-products";

    private SlowLogWorkload() {
    }

    public static void configureSlowLogs(SearchEngineHttp http) throws IOException, InterruptedException {
        http.put("/_cluster/settings", """
                {
                  "transient": {
                    "logger.index.search.slowlog": "TRACE",
                    "logger.index.indexing.slowlog": "TRACE"
                  }
                }
                """);

        http.put("/" + INDEX_NAME, """
                {
                  "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "index.search.slowlog.threshold.query.warn": "1ms",
                    "index.search.slowlog.threshold.query.trace": "0ms",
                    "index.search.slowlog.threshold.fetch.warn": "1ms",
                    "index.search.slowlog.threshold.fetch.trace": "0ms",
                    "index.indexing.slowlog.threshold.index.warn": "1ms",
                    "index.indexing.slowlog.threshold.index.trace": "0ms"
                  },
                  "mappings": {
                    "properties": {
                      "title": { "type": "text" },
                      "description": { "type": "text" },
                      "category": { "type": "keyword" },
                      "tags": { "type": "keyword" },
                      "price": { "type": "double" },
                      "payload": { "type": "text", "index": false }
                    }
                  }
                }
                """);
    }

    public static void seedDocuments(SearchEngineHttp http) throws IOException, InterruptedException {
        bulkIndex(http, buildSeedDocuments(2_000));
        http.post("/" + INDEX_NAME + "/_refresh", "");
    }

    public static void runHeavySearch(SearchEngineHttp http) throws IOException, InterruptedException {
        for (int i = 0; i < 5; i++) {
            http.post("/" + INDEX_NAME + "/_search", """
                    {
                      "size": 500,
                      "track_total_hits": true,
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "wildcard": {
                                "description": {
                                  "value": "*lorem*"
                                }
                              }
                            },
                            {
                              "script": {
                                "script": {
                                  "source": "double total = 0; for (int j = 0; j < 20000; j++) { total += Math.sqrt(j + doc['price'].value); } return total > 0;",
                                  "lang": "painless"
                                }
                              }
                            }
                          ]
                        }
                      },
                      "aggs": {
                        "categories": {
                          "terms": {
                            "field": "category",
                            "size": 10000
                          },
                          "aggs": {
                            "avg_price": {
                              "avg": { "field": "price" }
                            },
                            "tag_cardinality": {
                              "cardinality": {
                                "field": "tags",
                                "precision_threshold": 40000
                              }
                            }
                          }
                        }
                      }
                    }
                    """);
        }
    }

    public static void runHeavyIndexing(SearchEngineHttp http) throws IOException, InterruptedException {
        bulkIndex(http, buildHeavyIndexingDocuments(300));
        http.post("/" + INDEX_NAME + "/_refresh", "");
    }

    private static void bulkIndex(SearchEngineHttp http, List<String> ndjsonLines)
            throws IOException, InterruptedException {
        StringBuilder bulkBody = new StringBuilder();
        for (String line : ndjsonLines) {
            bulkBody.append(line).append('\n');
        }

        var response = http.post("/_bulk?refresh=false", bulkBody.toString());
        if (response.body().contains("\"errors\":true")) {
            throw new IllegalStateException("Bulk indexing reported errors: " + response.body());
        }
    }

    private static List<String> buildSeedDocuments(int count) {
        List<String> lines = new ArrayList<>(count * 2);
        String[] categories = {"electronics", "garden", "kitchen", "outdoors", "books", "toys"};
        for (int i = 0; i < count; i++) {
            String id = "seed-" + i;
            String category = categories[i % categories.length];
            lines.add("""
                    {"index":{"_index":"%s","_id":"%s"}}
                    """.formatted(INDEX_NAME, id).trim());
            lines.add("""
                    {
                      "title": "Product %d lorem ipsum widget",
                      "description": "lorem ipsum dolor sit amet category %s tag-%d variant-%d",
                      "category": "%s",
                      "tags": ["tag-%d", "tag-%d", "slowlog"],
                      "price": %d.%d
                    }
                    """.formatted(i, category, i % 97, i % 53, category, i % 211, i % 313, i % 1000, i % 100)
                    .replaceAll("\\s+", " "));
        }
        return lines;
    }

    private static List<String> buildHeavyIndexingDocuments(int count) {
        List<String> lines = new ArrayList<>(count * 2);
        String largePayload = "x".repeat(60_000);
        for (int i = 0; i < count; i++) {
            String id = "heavy-" + i;
            lines.add("""
                    {"index":{"_index":"%s","_id":"%s"}}
                    """.formatted(INDEX_NAME, id).trim());
            lines.add("""
                    {
                      "title": "Heavy ingest %d",
                      "description": "bulk ingest lorem ipsum %d with expensive payload",
                      "category": "bulk",
                      "tags": ["heavy", "ingest", "tag-%d"],
                      "price": %d.%d,
                      "payload": "%s"
                    }
                    """.formatted(i, i, i % 401, i % 500, i % 100, largePayload)
                    .replaceAll("\\s+", " "));
        }
        return lines;
    }
}
