package SlowLogDemo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Captures Elasticsearch 7.10 search and indexing slow logs for legacy dialect coverage in the blog viewer.
 */
@Tag("slowlog-capture")
@Tag("elasticsearch-7.10")
@EnabledIf("SlowLogDemo.DockerConditions#shouldRunSlowLogCapture")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Elasticsearch710SlowLogCaptureTest {

    private static final int HOST_PORT = 19202;
    private static final int CONTAINER_HTTP_PORT = 9200;
    private static final String LOGS_DIRECTORY = "/usr/share/elasticsearch/logs";

    private ElasticsearchContainer container;

    @BeforeAll
    void startContainer() {
        container = new ElasticsearchContainer(DockerImageName.parse(EngineVersions.ELASTICSEARCH_710_IMAGE))
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms768m -Xmx768m")
                .withStartupTimeout(Duration.ofMinutes(3));
        container.setPortBindings(List.of(HOST_PORT + ":" + CONTAINER_HTTP_PORT));
        container.start();
    }

    @AfterAll
    void stopContainer() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void capturesSearchAndIndexingSlowLogs() throws Exception {
        String baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(CONTAINER_HTTP_PORT);
        var result = SlowLogCaptureSession.capture(
                container,
                baseUrl,
                LOGS_DIRECTORY,
                "elasticsearch",
                EngineVersions.ELASTICSEARCH_710_VERSION);

        assertThat(result.searchSlowLog()).contains("slowlog");
    }
}
