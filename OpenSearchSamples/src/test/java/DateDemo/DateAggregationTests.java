package DateDemo;

import DateDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.aggregations.CardinalityAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for date field aggregations in OpenSearch.
 * These tests demonstrate how date fields can be used for various aggregation types.
 *
 * Date fields support aggregations including cardinality and other metric aggregations.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/date/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class DateAggregationTests {
    private static final Logger logger = LogManager.getLogger(DateAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public DateAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that date fields can be used for cardinality aggregation.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void dateMapping_CanBeUsedForCardinalityAggregation() throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", "2024-01-15T10:00:00Z"),
                    new ProductDocument(2, "Keyboard", "2024-06-15T12:00:00Z"),
                    new ProductDocument(3, "Monitor", "2024-12-15T14:00:00Z"),
                    new ProductDocument(4, "Cable", "2024-01-15T10:00:00Z"), // Same date as Mouse
                    new ProductDocument(5, "Headset", "2024-06-15T12:00:00Z") // Same date as Keyboard
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with cardinality aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("unique_dates", a -> a
                            .cardinality(c -> c
                                    .field("createdAt")
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            CardinalityAggregate cardinalityAgg = response.aggregations().get("unique_dates").cardinality();

            // There are 5 documents but only 3 unique dates
            assertThat(cardinalityAgg.value()).isEqualTo(3);
        }
    }
}

