package SlowLogDemo;

import org.testcontainers.containers.Container;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Reads slow-log output from a running search engine container.
 * <p>
 * Official OpenSearch/Elasticsearch Docker images typically write slow logs to
 * <strong>stdout</strong> (visible via {@code docker logs}), not to separate
 * {@code *slowlog*} files under the data/logs directory. This reader prefers
 * container logs and falls back to scanning the logs directory when possible.
 * </p>
 */
public final class SlowLogFileReader {

    private SlowLogFileReader() {
    }

    public static String readSlowLogs(Container<?> container, String logsDirectory) {
        String fromStdout = extractSlowLogLines(safeContainerLogs(container));
        if (!fromStdout.isBlank()) {
            return "# ---- container stdout/stderr ----\n" + fromStdout;
        }

        String fromFiles = readFromLogDirectory(container, logsDirectory);
        if (!fromFiles.isBlank()) {
            return fromFiles;
        }

        return "";
    }

    private static String safeContainerLogs(Container<?> container) {
        try {
            return container.getLogs();
        } catch (Exception e) {
            return "";
        }
    }

    static String extractSlowLogLines(String logs) {
        if (logs == null || logs.isBlank()) {
            return "";
        }
        StringBuilder filtered = new StringBuilder();
        for (String line : logs.split("\n")) {
            if (isSlowLogLine(line)) {
                filtered.append(line).append('\n');
            }
        }
        return filtered.toString().trim();
    }

    static boolean isSlowLogLine(String line) {
        if (line == null || line.isBlank()) {
            return false;
        }
        String lower = line.toLowerCase(Locale.ROOT);
        if (!(lower.contains("took_millis") || lower.contains("took_millis\":"))) {
            return false;
        }
        return lower.contains("slowlog")
                || lower.contains("i.s.s.query")
                || lower.contains("i.s.s.fetch")
                || lower.contains("i.i.s")
                || lower.contains("index.search.slowlog")
                || lower.contains("index.indexing.slowlog")
                || lower.contains("index_search_slowlog")
                || lower.contains("index_indexing_slowlog")
                || lower.contains("searchrequestslowlog")
                || lower.contains("c.s.r.slowlog");
    }

    private static String readFromLogDirectory(Container<?> container, String logsDirectory) {
        try {
            List<String> logFiles = listLogFiles(container, logsDirectory);
            if (logFiles.isEmpty()) {
                return "";
            }

            StringBuilder combined = new StringBuilder();
            for (String logFile : logFiles) {
                String content = readFile(container, logFile);
                String slow = extractSlowLogLines(content);
                if (!slow.isBlank()) {
                    combined.append("# ---- ").append(logFile).append(" ----\n");
                    combined.append(slow).append('\n');
                }
            }
            return combined.toString().trim();
        } catch (Exception e) {
            return "";
        }
    }

    private static List<String> listLogFiles(Container<?> container, String logsDirectory)
            throws Exception {
        // Prefer ls — many slim images omit `find`.
        var result = container.execInContainer(
                "sh",
                "-c",
                "ls -1 " + logsDirectory + " 2>/dev/null");
        if (result.getExitCode() != 0) {
            return List.of();
        }

        List<String> files = new ArrayList<>();
        for (String line : result.getStdout().split("\n")) {
            String name = line.trim();
            if (name.isEmpty()) {
                continue;
            }
            String lower = name.toLowerCase(Locale.ROOT);
            if (lower.contains("slow") || lower.endsWith(".log") || lower.endsWith(".json")) {
                files.add(logsDirectory + "/" + name);
            }
        }
        return files;
    }

    private static String readFile(Container<?> container, String path) throws Exception {
        var result = container.execInContainer("cat", path);
        if (result.getExitCode() != 0) {
            return "";
        }
        return result.getStdout();
    }
}
