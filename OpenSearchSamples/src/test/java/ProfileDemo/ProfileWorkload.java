package ProfileDemo;

import SlowLogDemo.SearchEngineHttp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Seeds data and runs an intentionally expensive search with {@code "profile": true}.
 */
public final class ProfileWorkload {

    public static final String INDEX_NAME = "profile-demo-products";

    private ProfileWorkload() {
    }

    public static void createIndex(SearchEngineHttp http) throws IOException, InterruptedException {
        http.put("/" + INDEX_NAME, """
                {
                  "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                  },
                  "mappings": {
                    "properties": {
                      "title": { "type": "text" },
                      "description": { "type": "text" },
                      "category": { "type": "keyword" },
                      "tags": { "type": "keyword" },
                      "price": { "type": "double" }
                    }
                  }
                }
                """);
    }

    public static void seedDocuments(SearchEngineHttp http) throws IOException, InterruptedException {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            String category = "cat-" + (i % 20);
            String doc = """
                    {"title":"Product %d","description":"lorem ipsum dolor sit amet %d","category":"%s","tags":["tag-a","tag-b"],"price":%d.5}
                    """.formatted(i, i, category, i % 100).trim();
            lines.add("{ \"index\": { \"_index\": \"" + INDEX_NAME + "\" } }");
            lines.add(doc);
        }
        StringBuilder bulk = new StringBuilder();
        for (String line : lines) {
            bulk.append(line).append('\n');
        }
        http.post("/_bulk?refresh=true", bulk.toString());
    }

    /** Heavy search body with profiling enabled — returns the raw HTTP JSON response. */
    public static String runProfiledHeavySearch(SearchEngineHttp http)
            throws IOException, InterruptedException {
        var response = http.post("/" + INDEX_NAME + "/_search", """
                {
                  "profile": true,
                  "size": 100,
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
                              "source": "double total = 0; for (int j = 0; j < 8000; j++) { total += Math.sqrt(j + doc['price'].value); } return total > 0;",
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
                        "size": 50
                      },
                      "aggs": {
                        "avg_price": {
                          "avg": { "field": "price" }
                        }
                      }
                    }
                  }
                }
                """);
        return response.body();
    }
}
