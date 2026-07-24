package SlowLogDemo;

/**
 * Pinned engine image tags for slow-log fixture capture.
 * <p>
 * Update these when refreshing blog fixtures so captured files and documentation stay aligned.
 * </p>
 */
public final class EngineVersions {

    /** Latest OpenSearch image used for slow-log capture (matches shared demo container family). */
    public static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.19.3";
    public static final String OPENSEARCH_VERSION = "2.19.3";

    /** Latest Elasticsearch 8.x image used for slow-log capture. */
    public static final String ELASTICSEARCH_LATEST_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.18.0";
    public static final String ELASTICSEARCH_LATEST_VERSION = "8.18.0";

    /** Legacy Elasticsearch 7.10 image for dialect coverage in the blog viewer. */
    public static final String ELASTICSEARCH_710_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.10.2";
    public static final String ELASTICSEARCH_710_VERSION = "7.10.2";

    private EngineVersions() {
    }
}
