package ProfileDemo;

import SlowLogDemo.EngineVersions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Captures a real OpenSearch Profile API response for the CodeSloth profile viewer.
 */
@Tag("profile-capture")
@Tag("opensearch")
@EnabledIf("ProfileDemo.ProfileDockerConditions#shouldRunProfileCapture")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenSearchProfileCaptureTest {

    private static final int HOST_PORT = 19300;
    private static final int CONTAINER_HTTP_PORT = 9200;

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
    void capturesSearchProfileJson() throws Exception {
        String baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(CONTAINER_HTTP_PORT);
        Path out = ProfileCaptureSession.capture(
                baseUrl, "opensearch", EngineVersions.OPENSEARCH_VERSION);
        assertThat(Files.exists(out)).isTrue();
        String json = Files.readString(out);
        assertThat(json).contains("\"profile\"");
        assertThat(json).contains("\"request\"");
        assertThat(json).contains("\"response\"");
        assertThat(json).contains("time_in_nanos");
    }
}
