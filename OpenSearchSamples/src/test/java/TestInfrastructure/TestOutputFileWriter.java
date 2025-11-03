package TestInfrastructure;

import com.fasterxml.jackson.core.JsonGenerator;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.PlainJsonSerializable;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for writing HTTP request/response interactions to files during test execution.
 * This allows capturing all OpenSearch interactions to avoid re-running slow tests.
 */
public class TestOutputFileWriter {
    private static final String OUTPUT_DIR = "test-outputs";
    private static final Map<String, StringBuilder> testOutputs = new HashMap<>();
    private static boolean captureEnabled = false;
    
    /**
     * Enables output capture for all tests.
     */
    public static void enableCapture() {
        captureEnabled = true;
        try {
            Path outputPath = Paths.get(OUTPUT_DIR);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directory: " + OUTPUT_DIR, e);
        }
    }
    
    /**
     * Disables output capture.
     */
    public static void disableCapture() {
        captureEnabled = false;
    }
    
    /**
     * Writes a request to the test output file.
     * 
     * @param testMethodName The name of the test method (used to organize output by test)
     * @param requestType The type of request (e.g., "PUT", "POST", "GET")
     * @param endpoint The endpoint or URL
     * @param requestBody The request body as JSON string
     */
    public static void writeRequest(String testMethodName, String requestType, String endpoint, String requestBody) {
        if (!captureEnabled) return;
        
        StringBuilder output = testOutputs.computeIfAbsent(testMethodName, k -> new StringBuilder());
        output.append("=== REQUEST ===\n");
        output.append("Method: ").append(requestType).append("\n");
        output.append("Endpoint: ").append(endpoint).append("\n");
        output.append("Request Body:\n");
        output.append(requestBody).append("\n\n");
    }
    
    /**
     * Writes a response to the test output file.
     * 
     * @param testMethodName The name of the test method
     * @param responseBody The response body as JSON string
     */
    public static void writeResponse(String testMethodName, String responseBody) {
        if (!captureEnabled) return;
        
        StringBuilder output = testOutputs.computeIfAbsent(testMethodName, k -> new StringBuilder());
        output.append("=== RESPONSE ===\n");
        output.append(responseBody).append("\n\n");
    }
    
    /**
     * Writes a search request and response together.
     * 
     * @param testMethodName The name of the test method
     * @param requestBody The search request as JSON string
     * @param responseBody The search response as JSON string
     */
    public static void writeSearchInteraction(String testMethodName, String requestBody, String responseBody) {
        if (!captureEnabled) return;
        
        StringBuilder output = testOutputs.computeIfAbsent(testMethodName, k -> new StringBuilder());
        output.append("=== SEARCH REQUEST ===\n");
        output.append(requestBody).append("\n\n");
        output.append("=== SEARCH RESPONSE ===\n");
        output.append(responseBody).append("\n\n");
    }
    
    /**
     * Serializes and writes a PlainJsonSerializable request to file.
     */
    public static void writeRequestJson(String testMethodName, String requestType, String endpoint, PlainJsonSerializable serializable) {
        if (!captureEnabled) return;
        writeRequest(testMethodName, requestType, endpoint, serializable.toJsonString());
    }
    
    /**
     * Serializes and writes a request object using the OpenSearch client's mapper.
     */
    public static <T> void writeRequestJson(OpenSearchClient client, String testMethodName, String requestType, String endpoint, T request) {
        if (!captureEnabled) return;
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                if (jsonGenerator instanceof JsonGenerator) {
                    ((JsonGenerator) jsonGenerator).useDefaultPrettyPrinter();
                }
                jsonpMapper.serialize(request, jsonGenerator);
            }
            writeRequest(testMethodName, requestType, endpoint, stringWriter.toString());
        } catch (Exception e) {
            writeRequest(testMethodName, requestType, endpoint, "Error serializing request: " + e.getMessage());
        }
    }
    
    /**
     * Serializes and writes a response object using the OpenSearch client's mapper.
     */
    public static <T> void writeResponseJson(OpenSearchClient client, String testMethodName, T response) {
        if (!captureEnabled) return;
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                if (jsonGenerator instanceof JsonGenerator) {
                    ((JsonGenerator) jsonGenerator).useDefaultPrettyPrinter();
                }
                jsonpMapper.serialize(response, jsonGenerator);
            }
            writeResponse(testMethodName, stringWriter.toString());
        } catch (Exception e) {
            writeResponse(testMethodName, "Error serializing response: " + e.getMessage());
        }
    }
    
    /**
     * Writes all captured outputs to files and clears the cache.
     * Should be called after each test method.
     */
    public static void flushTestOutput(String testMethodName) {
        if (!captureEnabled) return;
        
        StringBuilder output = testOutputs.remove(testMethodName);
        if (output == null || output.length() == 0) return;
        
        try {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = testMethodName + "_" + timestamp + ".txt";
            Path filePath = Paths.get(OUTPUT_DIR, fileName);
            
            try (FileWriter writer = new FileWriter(filePath.toFile())) {
                writer.write(output.toString());
            }
        } catch (IOException e) {
            System.err.println("Failed to write test output for " + testMethodName + ": " + e.getMessage());
        }
    }
    
    /**
     * Clears all cached outputs (useful for cleanup).
     */
    public static void clearAll() {
        testOutputs.clear();
    }
}

