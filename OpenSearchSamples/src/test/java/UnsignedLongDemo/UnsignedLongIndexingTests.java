package UnsignedLongDemo;

import UnsignedLongDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch.core.GetResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for unsigned_long field indexing in OpenSearch.
 * Unsigned_long fields store unsigned 64-bit integers (0 to 18,446,744,073,709,551,615).
 * In Java, these are represented as signed long values.
 * 
 * Note: unsigned_long field type was introduced in OpenSearch 2.4.0
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class UnsignedLongIndexingTests {
    private static final Logger logger = LogManager.getLogger(UnsignedLongIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public UnsignedLongIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 'Zero value is indexed correctly'",
            "1000, 'Positive value is indexed correctly'",
            "9223372036854775807, 'Large positive value (max signed long) is indexed correctly'",
            "100000000000, 'Large positive value is indexed correctly'"
    })
    public void unsignedLongMapping_IndexesNumericValueCorrectly(long stockValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.unsignedLong(ul -> ul))))) {

            ProductDocument productDocument = new ProductDocument(1, "Mouse", stockValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getStock()).as(explanation).isEqualTo(stockValue);
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }
}

