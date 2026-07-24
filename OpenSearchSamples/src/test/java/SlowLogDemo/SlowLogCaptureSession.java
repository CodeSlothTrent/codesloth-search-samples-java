package SlowLogDemo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end slow-log capture for a single search engine container.
 */
public final class SlowLogCaptureSession {

    private static final Logger logger = LogManager.getLogger(SlowLogCaptureSession.class);

    private SlowLogCaptureSession() {
    }

    public record CaptureResult(String searchSlowLog, String indexingSlowLog, PathSummary pathSummary) {
    }

    public record PathSummary(String searchFixturePath, String indexingFixturePath) {
    }

    public static CaptureResult capture(
            Container<?> container,
            String baseUrl,
            String logsDirectory,
            String engineSlug,
            String version) throws Exception {

        try (SearchEngineHttp http = new SearchEngineHttp(baseUrl)) {
            http.waitForClusterHealth();
            SlowLogWorkload.configureSlowLogs(http);
            SlowLogWorkload.seedDocuments(http);
            SlowLogWorkload.runHeavySearch(http);
            SlowLogWorkload.runHeavyIndexing(http);

            // Give the engine a moment to flush slow-log files.
            Thread.sleep(Duration.ofSeconds(3).toMillis());

            String allSlowLogs = SlowLogFileReader.readSlowLogs(container, logsDirectory);
            assertThat(allSlowLogs)
                    .as("Expected at least one slow-log line from %s %s", engineSlug, version)
                    .isNotBlank();

            String searchSlowLog = filterSlowLogLines(allSlowLogs, "search");
            String indexingSlowLog = filterSlowLogLines(allSlowLogs, "indexing");

            assertThat(searchSlowLog)
                    .as("Expected search slow-log lines from %s %s", engineSlug, version)
                    .isNotBlank();

            if (!indexingSlowLog.isBlank()) {
                assertThat(indexingSlowLog)
                        .as("Expected indexing slow-log lines from %s %s", engineSlug, version)
                        .contains("slowlog");
            } else {
                logger.warn("No indexing slow-log lines captured for {} {} (search slow logs were captured)", engineSlug, version);
            }

            PathSummary paths = new PathSummary(null, null);
            if (SlowLogFixtureWriter.isCaptureEnabled()) {
                var searchPath = SlowLogFixtureWriter.writeFixture(engineSlug, version, "search", searchSlowLog);
                paths = new PathSummary(searchPath.toString(), null);
                logger.info("Wrote search slow-log fixture to {}", searchPath);
                if (!indexingSlowLog.isBlank()) {
                    var indexingPath = SlowLogFixtureWriter.writeFixture(engineSlug, version, "indexing", indexingSlowLog);
                    paths = new PathSummary(searchPath.toString(), indexingPath.toString());
                    logger.info("Wrote indexing slow-log fixture to {}", indexingPath);
                }
            }

            return new CaptureResult(searchSlowLog, indexingSlowLog, paths);
        }
    }

    private static String filterSlowLogLines(String content, String kind) {
        StringBuilder filtered = new StringBuilder();
        boolean includeCommentsForKind = false;
        for (String line : content.split("\n")) {
            if (line.startsWith("# ----")) {
                includeCommentsForKind = line.toLowerCase().contains(kind)
                        || line.contains("stdout")
                        || line.contains("stderr");
                if (includeCommentsForKind) {
                    filtered.append(line).append('\n');
                }
                continue;
            }
            if (line.startsWith("#")) {
                if (includeCommentsForKind) {
                    filtered.append(line).append('\n');
                }
                continue;
            }
            if (!SlowLogFileReader.isSlowLogLine(line)) {
                continue;
            }
            String lower = line.toLowerCase();
            boolean isIndexing = lower.contains("indexing")
                    || lower.contains("i.i.s")
                    || lower.contains("index_indexing_slowlog");
            boolean isSearch = lower.contains("search")
                    || lower.contains("i.s.s.")
                    || lower.contains("index_search_slowlog")
                    || lower.contains("c.s.r.slowlog")
                    || lower.contains("searchrequestslowlog");
            if ("indexing".equals(kind) && isIndexing) {
                filtered.append(line).append('\n');
            } else if ("search".equals(kind) && isSearch && !isIndexing) {
                filtered.append(line).append('\n');
            } else if ("search".equals(kind) && !isIndexing && lower.contains("took_millis")) {
                // Fallback: treat non-indexing slow lines as search.
                filtered.append(line).append('\n');
            }
        }
        return filtered.toString().trim();
    }
}
