package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field scripting in OpenSearch.
 * These tests demonstrate how keyword fields can be used with scripts.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordScriptingTests {
    private static final Logger logger = LogManager.getLogger(KeywordScriptingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordScriptingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that keyword fields can be used to create a scripted field.
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

            // Create a search request with a script field
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .scriptFields("category", sf -> sf
                            .script(s -> s
                                    .inline(i -> i
                                            .source("doc['name'].value == 'mouse' ? 'computer accessory' : 'mouse accessory'")
                                    )
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(2);

            // Format the results as a string with the format "name:category"
            StringBuilder formattedResults = new StringBuilder();
            for (int i = 0; i < response.hits().hits().size(); i++) {
                var hit = response.hits().hits().get(i);
                var productDocument = hit.source();
                var scriptedField = hit.fields().get("category");

                assertThat(scriptedField).isNotNull();

                if (i > 0) {
                    formattedResults.append(", ");
                }
                formattedResults.append(productDocument.getName())
                        .append(":")
                        .append(scriptedField.toString()); //.get(0).toString());
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
                    new ProductDocument(2, "keyboard", 2),
                    new ProductDocument(3, "monitor", 3)
            };
            testIndex.indexDocuments(productDocuments);

            // Define a variable to be interpolated into the script
            String scriptedVariableValue = "mouse";

            // Create a search request with a script field using string interpolation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .scriptFields("category", sf -> sf
                            .script(s -> s
                                    .inline(i -> i
                                            .source("doc['name'].value == '" + scriptedVariableValue + "' ? 'computer accessory' : 'other accessory'")
                                    )
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(3);

            // Check that each document has the correct category based on the interpolated variable
            for (int i = 0; i < response.hits().hits().size(); i++) {
                var hit = response.hits().hits().get(i);
                var productDocument = hit.source();
                var scriptedField = hit.fields().get("category");

                assertThat(scriptedField).isNotNull();

                String expectedCategory = productDocument.getName().equals(scriptedVariableValue)
                        ? "computer accessory"
                        : "other accessory";

                assertThat(scriptedField.toString()).isEqualTo(expectedCategory); //.get(0).toString()
            }
        }
    }
} 