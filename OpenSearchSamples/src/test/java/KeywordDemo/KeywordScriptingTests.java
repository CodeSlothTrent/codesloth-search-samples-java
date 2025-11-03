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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.SourceFilter;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field scripting in OpenSearch.
 * These tests demonstrate how keyword fields can be used with scripts.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordScriptingTests {
    private static final Logger logger = LogManager.getLogger(KeywordScriptingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordScriptingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that keyword fields can be used to create a scripted field.
     * <p>
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedToCreateAScriptedField() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with a script field
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .scriptFields("category", sf -> sf
                            .script(script -> script
                                    .inline(i -> i
                                            .source("doc['name'].value == 'mouse' ? 'computer accessory' : 'mouse accessory'")
                                    )
                            )
                    )
                    .source(b -> b.filter(new SourceFilter.Builder().build())),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(2);

            // Format the results as a string with the format "name:category"
            StringBuilder formattedResults = new StringBuilder();
            for (int i = 0; i < response.hits().hits().size(); i++) {
                var hit = response.hits().hits().get(i);
                var productDocument = hit.source();
                String category = hit.fields().get("category").toString().replaceAll("[\\[\\]\"]", "");

                assertThat(category).isNotNull();

                if (i > 0) {
                    formattedResults.append(", ");
                }
                formattedResults.append(productDocument.getName())
                        .append(":")
                        .append(category.toString()); //.get(0).toString());
            }

            // Verify the formatted results
            assertThat(formattedResults.toString()).isEqualTo("mouse:computer accessory, mouse pad:mouse accessory");
        }
    }

    /**
     * This test verifies that keyword fields can be used to create a scripted field from an interpolated variable.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedToCreateAScriptedField_FromAnInterpolatedVariable() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Define a variable to be interpolated into the script
            String scriptedVariableValue = "mouse";

            // Execute the search request with a script field using string interpolation
            SearchResponse<ProductDocument> searchResponse = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(query -> query.matchAll(new MatchAllQuery.Builder().build()))
                    .scriptFields("category", sf -> sf
                            .script(script -> script
                                    .inline(i -> i
                                            .source("doc['name'].value == '" + scriptedVariableValue + "' ? 'computer accessory' : 'mouse accessory'")
                                    )
                            )
                    )
                    // The source field is omitted from results, so we must set an empty filter to fetch it
                    .source(b -> b.filter(new SourceFilter.Builder().build())),
                    ProductDocument.class);

            // Verify the results
            assertThat(searchResponse.hits().total().value()).isEqualTo(2);

            // Format the results for easier verification
            StringBuilder formattedResults = new StringBuilder();
            searchResponse.hits().hits().forEach(hit -> {
                String name = hit.source().getName();
                String category = hit.fields().get("category").toString().replaceAll("[\\[\\]\"]", "");
                if (formattedResults.length() > 0) {
                    formattedResults.append(", ");
                }
                formattedResults.append(name).append(":").append(category);
            });

            // Verify the formatted results
            assertThat(formattedResults.toString()).isEqualTo("mouse:computer accessory, mouse pad:mouse accessory");
        }
    }
} 