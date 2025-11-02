package TestInfrastructure;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.logging.log4j.LogManager;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.PlainJsonSerializable;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.StringWriter;

public class OpenSearchRequestLogger {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(OpenSearchRequestLogger.class);

    public static void LogRequestJson(PlainJsonSerializable serializable) {
        logger.info(serializable.toJsonString());
    }

    public static <T> void LogRequestJson(OpenSearchClient client, T request) {
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
        } catch (Exception e) {
            logger.error("Failed to serialize request: " + e.getMessage(), e);
        }
    }

    public static <T> void LogSearchRequest(OpenSearchClient client, T request) {
        logger.info("Search Request:");
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
        } catch (Exception e) {
            logger.error("Failed to serialize request: " + e.getMessage(), e);
        }
    }

    public static <T> void LogResponseJson(OpenSearchClient client, T response) {
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
        } catch (Exception e) {
            logger.error("Failed to serialize response: " + e.getMessage(), e);
        }
    }

    public static <T> void LogSearchResponse(OpenSearchClient client, T response) {
        logger.info("Search Response:");
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
        } catch (Exception e) {
            logger.error("Failed to serialize response: " + e.getMessage(), e);
        }
    }
}
