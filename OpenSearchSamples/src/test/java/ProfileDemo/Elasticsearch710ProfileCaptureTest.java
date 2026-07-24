package ProfileDemo;

import SlowLogDemo.EngineVersions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("profile-capture")
@Tag("elasticsearch-7.10")
@EnabledIf("ProfileDemo.ProfileDockerConditions#shouldRunProfileCapture")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Elasticsearch710ProfileCaptureTest {

    private static final int HOST_PORT = 19302;
    private static final int CONTAINER_HTTP_PORT = 9200;

    private ElasticsearchContainer container;

    @BeforeAll
    void startContainer() {
        container = new ElasticsearchContainer(
                DockerImageName.parse(EngineVersions.ELASTICSEARCH_710_IMAGE));
        container.withEnv("discovery.type", "single-node");
        container.withEnv("xpack.security.enabled", "false");
        container.withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        container.withStartupTimeout(Duration.ofMinutes(3));
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
    void capturesSearchProfileJson() throws Exception {
        String baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(CONTAINER_HTTP_PORT);
        Path out = ProfileCaptureSession.capture(
                baseUrl, "elasticsearch", EngineVersions.ELASTICSEARCH_710_VERSION);
        assertThat(Files.exists(out)).isTrue();
        assertThat(Files.readString(out)).contains("\"profile\"");
    }
}
