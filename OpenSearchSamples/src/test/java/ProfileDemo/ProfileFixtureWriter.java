package ProfileDemo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Writes captured Profile API JSON to {@code test-outputs/profile/} when capture is enabled.
 */
public final class ProfileFixtureWriter {

    public static final String OUTPUT_DIR = "test-outputs/profile";
    public static final String CAPTURE_PROPERTY = "profile.enableCapture";

    private ProfileFixtureWriter() {
    }

    public static boolean isCaptureEnabled() {
        return Boolean.parseBoolean(System.getProperty(CAPTURE_PROPERTY, "false"));
    }

    public static Path writeFixture(String engineSlug, String version, String jsonBody)
            throws IOException {
        Path outputDir = Paths.get(OUTPUT_DIR);
        Files.createDirectories(outputDir);

        String fileName = engineSlug + "-" + version + "-search-profile.json";
        Path outputPath = outputDir.resolve(fileName);

        // Pretty-print lightly via identity — body is already JSON from the engine.
        String headerComment = """
                /* Captured by ProfileDemo integration tests
                 * engine=%s version=%s
                 * capturedAt=%s
                 * Re-copy to codesloth-blog/public/tools/profile/ for the blog viewer.
                 */
                """.formatted(
                engineSlug,
                version,
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        // JSON files cannot include /* */ comments — write pure JSON only.
        Files.writeString(outputPath, jsonBody.trim() + "\n", StandardCharsets.UTF_8);

        Path metaPath = outputDir.resolve(engineSlug + "-" + version + "-search-profile.meta.txt");
        Files.writeString(metaPath, headerComment, StandardCharsets.UTF_8);

        return outputPath;
    }
}
