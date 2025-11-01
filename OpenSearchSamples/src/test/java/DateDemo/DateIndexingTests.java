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
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.GetResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for date field indexing in OpenSearch.
 * These tests demonstrate how date fields are indexed and stored in OpenSearch.
 *
 * Date fields store date/time values and support various date formats.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/date/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class DateIndexingTests {
    private static final Logger logger = LogManager.getLogger(DateIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public DateIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that date fields are indexed correctly using Instant.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void dateMapping_IndexesDateValueCorrectly() throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            // Create and index a product document with a date
            String creationDate = "2024-01-15T10:30:00Z";
            ProductDocument productDocument = new ProductDocument(1, "Mouse", creationDate);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Retrieve the document
            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getCreatedAt()).as("Date should be indexed correctly")
                    .isEqualTo(creationDate);
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }

    /**
     * This test verifies that date fields can handle different date values.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void dateMapping_HandlesVariousDateValues() throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            // Create documents with different dates
            String date1 = "2024-01-01T00:00:00Z";
            String date2 = "2024-12-31T23:59:59Z";
            String date3 = "2023-06-15T12:30:45Z";

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", date1),
                    new ProductDocument(2, "Keyboard", date2),
                    new ProductDocument(3, "Monitor", date3)
            };
            testIndex.indexDocuments(productDocuments);

            // Verify each document
            for (ProductDocument doc : productDocuments) {
                GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                        .index(testIndex.getName())
                        .id(doc.getId()),
                        ProductDocument.class
                );

                assertThat(result.found()).isTrue();
                assertThat(result.source().getCreatedAt())
                        .as("Date should be preserved for document " + doc.getId())
                        .isEqualTo(doc.getCreatedAt());
            }
        }
    }
}

