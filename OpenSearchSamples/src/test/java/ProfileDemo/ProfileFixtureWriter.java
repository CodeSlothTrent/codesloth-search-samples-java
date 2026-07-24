package ProfileDemo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Writes captured Profile API envelopes to {@code test-outputs/profile/} when capture is enabled.
 * <p>
 * Envelope shape (CodeSloth blog viewer):
 * <pre>
 * { "request": { ...search body... }, "response": { ...hits + profile... } }
 * </pre>
 */
public final class ProfileFixtureWriter {

    public static final String OUTPUT_DIR = "test-outputs/profile";
    public static final String CAPTURE_PROPERTY = "profile.enableCapture";

    private ProfileFixtureWriter() {
    }

    public static boolean isCaptureEnabled() {
        return Boolean.parseBoolean(System.getProperty(CAPTURE_PROPERTY, "false"));
    }

    public static Path writeFixture(
            String engineSlug, String version, String requestJson, String responseJson)
            throws IOException {
        Path outputDir = Paths.get(OUTPUT_DIR);
        Files.createDirectories(outputDir);

        String fileName = engineSlug + "-" + version + "-search-profile.json";
        Path outputPath = outputDir.resolve(fileName);

        // Build envelope without requiring a JSON library — request/response are already JSON objects.
        String envelope = """
                {
                  "request": %s,
                  "response": %s
                }
                """.formatted(requestJson.trim(), responseJson.trim());

        Files.writeString(outputPath, envelope + "\n", StandardCharsets.UTF_8);

        Path metaPath = outputDir.resolve(engineSlug + "-" + version + "-search-profile.meta.txt");
        String meta = """
                Captured by ProfileDemo integration tests
                engine=%s version=%s
                capturedAt=%s
                format=request+response envelope
                Re-copy to codesloth-blog/public/tools/profile/ for the blog viewer.
                """.formatted(
                engineSlug,
                version,
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        Files.writeString(metaPath, meta, StandardCharsets.UTF_8);

        return outputPath;
    }
}
