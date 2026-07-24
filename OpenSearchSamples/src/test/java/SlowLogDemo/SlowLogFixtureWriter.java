package SlowLogDemo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Writes captured slow-log lines to {@code test-outputs/slowlog/} when capture is enabled.
 */
public final class SlowLogFixtureWriter {

    public static final String OUTPUT_DIR = "test-outputs/slowlog";
    public static final String CAPTURE_PROPERTY = "slowlog.enableCapture";

    private SlowLogFixtureWriter() {
    }

    public static boolean isCaptureEnabled() {
        return Boolean.parseBoolean(System.getProperty(CAPTURE_PROPERTY, "false"));
    }

    public static Path writeFixture(String engineSlug, String version, String logKind, String content)
            throws IOException {
        Path outputDir = Paths.get(OUTPUT_DIR);
        Files.createDirectories(outputDir);

        String fileName = engineSlug + "-" + version + "-" + logKind + "-slowlog.log";
        Path outputPath = outputDir.resolve(fileName);

        String header = """
                # Captured by SlowLogDemo integration tests
                # engine=%s version=%s kind=%s
                # capturedAt=%s
                # Re-copy to codesloth-blog/public/tools/slow-log/ for the blog viewer.

                """.formatted(
                engineSlug,
                version,
                logKind,
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        Files.writeString(outputPath, header + content, StandardCharsets.UTF_8);
        return outputPath;
    }
}
