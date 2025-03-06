package TestInfrastructure;

import org.apache.logging.log4j.LogManager;
import org.opensearch.client.json.PlainJsonSerializable;

public class OpenSearchRequestLogger {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(OpenSearchRequestLogger.class);

    public static void LogRequestJson(PlainJsonSerializable serializable) {
        logger.info(serializable.toJsonString());
    }
}
