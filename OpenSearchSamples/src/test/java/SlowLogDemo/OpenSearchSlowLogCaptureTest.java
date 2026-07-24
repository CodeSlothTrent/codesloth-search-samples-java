package SlowLogDemo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Captures real OpenSearch search and indexing slow logs for the CodeSloth blog viewer.
 * <p>
 * Uses a dedicated container on port {@value #HOST_PORT} so it does not interfere with the shared
 * OpenSearch singleton used by the rest of the demo tests.
 * </p>
 */
@Tag("slowlog-capture")
@Tag("opensearch")
@EnabledIf("SlowLogDemo.DockerConditions#shouldRunSlowLogCapture")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenSearchSlowLogCaptureTest {

    private static final int HOST_PORT = 19200;
    private static final int CONTAINER_HTTP_PORT = 9200;
    private static final String LOGS_DIRECTORY = "/usr/share/opensearch/logs";

    private OpensearchContainer<?> container;

    @BeforeAll
    void startContainer() {
        container = new OpensearchContainer<>(EngineVersions.OPENSEARCH_IMAGE);
        container.withEnv("discovery.type", "single-node");
        container.withEnv("OPENSEARCH_JAVA_OPTS", "-Xms768m -Xmx768m");
        container.withEnv("DISABLE_SECURITY_PLUGIN", "true");
        container.withStartupTimeout(Duration.ofMinutes(3));
        container.setPortBindings(List.of(HOST_PORT + ":" + CONTAINER_HTTP_PORT));
        container.waitingFor(Wait.forHttp("/").forPort(CONTAINER_HTTP_PORT).forStatusCode(200));
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
                "opensearch",
                EngineVersions.OPENSEARCH_VERSION);

        assertThat(result.searchSlowLog()).contains("slowlog");
    }
}
