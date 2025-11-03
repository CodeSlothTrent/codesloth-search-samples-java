package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.TermvectorsResponse;
import org.opensearch.client.opensearch.core.termvectors.TermVector;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field indexing in OpenSearch.
 * These tests demonstrate how keyword fields are indexed and stored in OpenSearch.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordIndexingTests {
    private static final Logger logger = LogManager.getLogger(KeywordIndexingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that keyword fields are indexed exactly as given, without tokenization.
     * <p>
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     *
     * @param termText    The text to index
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "Mouse, 'Single word is indexed exactly as given'",
            "'Mouse pad', 'Two words are indexed exactly as given'",
            "'This is a sentence! It contains some, really bad. Grammar;', 'All grammar is indexed exactly as given'"
    })
    public void keywordMapping_IndexesASingleTokenForGivenString(String termText, String explanation) throws Exception {
        // Create a test index with keyword mapping for the name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, termText, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Get term vectors for the document
            TermvectorsResponse result = loggingOpenSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(String.valueOf(productDocument.getId()))
                    .fields("name")
            );

            // Verify the results
            assertThat(result.found()).isTrue();

            var resultString = result.toJsonString();

            // Extract tokens and their frequencies
            Map<String, TermVector> termVectors = result.termVectors();
            String tokensAndFrequency = termVectors.entrySet().stream()
                    .flatMap(entry -> entry.getValue().terms().entrySet().stream()
                            .map(term -> term.getKey() + ":" + term.getValue().termFreq()))
                    .collect(Collectors.joining(", "));

            String expectedTokenCsv = termText + ":1";
            assertThat(tokensAndFrequency).as(explanation).isEqualTo(expectedTokenCsv);
        }
    }
} 