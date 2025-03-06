package TestInfrastructure;

import com.fasterxml.jackson.core.JsonFactory;
import jakarta.json.stream.JsonGenerator;
import org.apache.logging.log4j.LogManager;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;

import java.io.IOException;
import java.io.StringWriter;

public class OpenSearchRequestLogger {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(OpenSearchRequestLogger.class);

    public static void LogRequestJson(JsonpSerializable serializable) {
        StringWriter writer = new StringWriter();
        JsonGenerator generator = null;
        try {
            generator = new JacksonJsonpGenerator(new JsonFactory().createGenerator(writer));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        serializable.serialize(generator, new JacksonJsonpMapper());
        generator.flush();
        var json = writer.toString();
        logger.info(json);
    }
}
