package SlowLogDemo;

import org.testcontainers.DockerClientFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * JUnit {@link org.junit.jupiter.api.condition.EnabledIf} helpers for Docker-gated tests.
 * <p>
 * Docker Desktop 4.x / Engine 29+ rejects very old API versions (e.g. v1.32) with HTTP 400.
 * docker-java / Testcontainers may still negotiate those unless {@code api.version} is set.
 * This helper also points at {@code ~/.docker/run/docker.sock} when {@code /var/run/docker.sock}
 * is absent (common on macOS without the "default socket" Desktop setting).
 * </p>
 */
public final class DockerConditions {

    private static final String DOCKER_DESKTOP_SOCK =
            System.getProperty("user.home") + "/.docker/run/docker.sock";

    static {
        configureDockerClientDefaults();
    }

    private DockerConditions() {
    }

    static void configureDockerClientDefaults() {
        // Docker Engine 29 minimum usable API for /info is >= 1.41 in practice.
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
        if (System.getProperty("docker.host") == null
                && System.getenv("DOCKER_HOST") == null
                && !Files.exists(Path.of("/var/run/docker.sock"))
                && Files.exists(Path.of(DOCKER_DESKTOP_SOCK))) {
            System.setProperty("docker.host", "unix://" + DOCKER_DESKTOP_SOCK);
        }
    }

    public static boolean isDockerAvailable() {
        configureDockerClientDefaults();
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            return false;
        }
    }

    /** Used by {@link org.junit.jupiter.api.condition.EnabledIf} on slow-log capture tests. */
    public static boolean shouldRunSlowLogCapture() {
        return isDockerAvailable() && SlowLogFixtureWriter.isCaptureEnabled();
    }
}
