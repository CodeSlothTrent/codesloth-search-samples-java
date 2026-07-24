package ProfileDemo;

import SlowLogDemo.DockerConditions;

/** JUnit {@code EnabledIf} helpers for Profile API capture tests. */
public final class ProfileDockerConditions {

    private ProfileDockerConditions() {
    }

    public static boolean shouldRunProfileCapture() {
        return DockerConditions.isDockerAvailable() && ProfileFixtureWriter.isCaptureEnabled();
    }
}
