package TestInfrastructure;

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
                jsonpMapper.serialize(request, jsonGenerator);
            }
            String jsonRequest = stringWriter.toString();
            logger.info(jsonRequest);
        } catch (Exception e) {
            logger.error("Failed to serialize request: " + e.getMessage(), e);
        }
    }

    public static <T> void LogSearchRequest(OpenSearchClient client, T request) {
        System.out.println("Search Request:");
        logger.info("Search Request:");
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                jsonpMapper.serialize(request, jsonGenerator);
            }
            String jsonRequest = stringWriter.toString();
            System.out.println(jsonRequest);
            logger.info(jsonRequest);
        } catch (Exception e) {
            System.err.println("Failed to serialize request: " + e.getMessage());
            logger.error("Failed to serialize request: " + e.getMessage(), e);
        }
    }

    public static <T> void LogResponseJson(OpenSearchClient client, T response) {
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                jsonpMapper.serialize(response, jsonGenerator);
            }
            String jsonResponse = stringWriter.toString();
            logger.info(jsonResponse);
        } catch (Exception e) {
            logger.error("Failed to serialize response: " + e.getMessage(), e);
        }
    }

    public static <T> void LogSearchResponse(OpenSearchClient client, T response) {
        System.out.println("Search Response:");
        logger.info("Search Response:");
        try {
            JsonpMapper jsonpMapper = client._transport().jsonpMapper();
            StringWriter stringWriter = new StringWriter();
            try (var jsonGenerator = jsonpMapper.jsonProvider().createGenerator(stringWriter)) {
                jsonpMapper.serialize(response, jsonGenerator);
            }
            String jsonResponse = stringWriter.toString();
            System.out.println(jsonResponse);
            logger.info(jsonResponse);
        } catch (Exception e) {
            System.err.println("Failed to serialize response: " + e.getMessage());
            logger.error("Failed to serialize response: " + e.getMessage(), e);
        }
    }
}
