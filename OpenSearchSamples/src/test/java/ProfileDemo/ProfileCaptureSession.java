package ProfileDemo;

import SlowLogDemo.SearchEngineHttp;

import java.nio.file.Path;

/**
 * Shared capture flow: wait for cluster → index → profiled heavy search → write fixture.
 */
public final class ProfileCaptureSession {

    private ProfileCaptureSession() {
    }

    public static Path capture(String baseUrl, String engineSlug, String version) throws Exception {
        try (SearchEngineHttp http = new SearchEngineHttp(baseUrl)) {
            http.waitForClusterHealth();
            ProfileWorkload.createIndex(http);
            ProfileWorkload.seedDocuments(http);
            String body = ProfileWorkload.runProfiledHeavySearch(http);
            if (body == null || !body.contains("\"profile\"")) {
                throw new IllegalStateException("Search response did not include a profile section");
            }
            return ProfileFixtureWriter.writeFixture(engineSlug, version, body);
        }
    }
}
