package TestInfrastructure;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.logging.log4j.LogManager;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Instance-based logger for OpenSearch requests and responses.
 * Each instance is scoped to a test class and writes directly to a single file.
 * Supports both console logging (via log4j) and file output (for test documentation).
 */
public class OpenSearchRequestLogger {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(OpenSearchRequestLogger.class);
    private static final String OUTPUT_DIR = "test-outputs";
    
    private final String testClassName;
    private final boolean captureEnabled;
    private FileWriter fileWriter;
    private int requestCounter = 0;
    private int responseCounter = 0;
    
    /**
     * Creates a new logger instance for a test class.
     * Opens a single output file that will be used for all requests/responses from this test class.
     * 
     * @param testClassName The name of the test class (for file naming)
     * @param captureEnabled Whether to write to files (console logging always enabled)
     */
    public OpenSearchRequestLogger(String testClassName, boolean captureEnabled) {
        this.testClassName = testClassName;
        this.captureEnabled = captureEnabled;
        
        if (captureEnabled) {
            try {
                Path outputPath = Paths.get(OUTPUT_DIR);
                if (!Files.exists(outputPath)) {
                    Files.createDirectories(outputPath);
                }
                
                // Create a single file for this test class
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String fileName = testClassName + "_" + timestamp + ".txt";
                Path filePath = Paths.get(OUTPUT_DIR, fileName);
                this.fileWriter = new FileWriter(filePath.toFile(), false); // false = overwrite, true = append
            } catch (IOException e) {
                throw new RuntimeException("Failed to create output file for test class: " + testClassName, e);
            }
        }
    }

    /**
     * Logs a request (console and file).
     * Always logs to console and writes to file if capture is enabled.
     * 
     * @param client The OpenSearch client (required for serialization)
     * @param request The request object to serialize
     */
    public <T> void logRequest(OpenSearchClient client, T request) {
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                // Configure pretty printing if using Jackson
                if (jsonGenerator instanceof JsonGenerator) {
                    ((JsonGenerator) jsonGenerator).useDefaultPrettyPrinter();
                }
                jsonpMapper.serialize(request, jsonGenerator);
            }
            String jsonRequest = stringWriter.toString();
            logger.info(jsonRequest);
            if (captureEnabled) {
                writeRequestToFile(jsonRequest);
            }
        } catch (Exception e) {
            logger.error("Failed to serialize request: " + e.getMessage(), e);
            if (captureEnabled) {
                writeRequestToFile("Error serializing request: " + e.getMessage());
            }
        }
    }

    /**
     * Logs a response (console and file).
     * Always logs to console and writes to file if capture is enabled.
     * 
     * @param client The OpenSearch client (required for serialization)
     * @param response The response object to serialize
     */
    public <T> void logResponse(OpenSearchClient client, T response) {
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                // Configure pretty printing if using Jackson
                if (jsonGenerator instanceof JsonGenerator) {
                    ((JsonGenerator) jsonGenerator).useDefaultPrettyPrinter();
                }
                jsonpMapper.serialize(response, jsonGenerator);
            }
            String jsonResponse = stringWriter.toString();
            logger.info(jsonResponse);
            if (captureEnabled) {
                writeResponseToFile(jsonResponse);
            }
        } catch (Exception e) {
            logger.error("Failed to serialize response: " + e.getMessage(), e);
            if (captureEnabled) {
                writeResponseToFile("Error serializing response: " + e.getMessage());
            }
        }
    }

    /**
     * Logs an exception (console and file).
     * Always logs to console and writes to file if capture is enabled.
     * 
     * @param exception The exception to log
     */
    public void logException(Exception exception) {
        String exceptionMessage = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        String stackTrace = getStackTrace(exception);
        
        logger.error(exceptionMessage, exception);
        
        if (captureEnabled) {
            responseCounter++;
            try {
                fileWriter.write("=== EXCEPTION #" + responseCounter + " ===\n");
                fileWriter.write(exceptionMessage);
                fileWriter.write("\n");
                fileWriter.write(stackTrace);
                fileWriter.write("\n\n");
                fileWriter.flush();
            } catch (IOException e) {
                System.err.println("Failed to write exception to file: " + e.getMessage());
            }
        }
    }
    
    private String getStackTrace(Exception exception) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }

    // Private helper methods for file writing
    
    private synchronized void writeRequestToFile(String requestBody) {
        if (fileWriter == null) {
            return;
        }
        
        requestCounter++;
        try {
            fileWriter.write("=== REQUEST #" + requestCounter + " ===\n");
            fileWriter.write(requestBody);
            fileWriter.write("\n\n");
            fileWriter.flush(); // Flush after each write to ensure data is written
        } catch (IOException e) {
            System.err.println("Failed to write request to file: " + e.getMessage());
        }
    }
    
    private synchronized void writeResponseToFile(String responseBody) {
        if (fileWriter == null) {
            return;
        }
        
        responseCounter++;
        try {
            fileWriter.write("=== RESPONSE #" + responseCounter + " ===\n");
            fileWriter.write(responseBody);
            fileWriter.write("\n\n");
            fileWriter.flush(); // Flush after each write to ensure data is written
        } catch (IOException e) {
            System.err.println("Failed to write response to file: " + e.getMessage());
        }
    }
    
    /**
     * Closes the output file. Should be called when the test class is done executing.
     * This ensures all buffered data is written to disk.
     */
    public synchronized void close() {
        if (fileWriter != null) {
            try {
                fileWriter.close();
                fileWriter = null;
            } catch (IOException e) {
                System.err.println("Failed to close output file: " + e.getMessage());
            }
        }
    }
}
