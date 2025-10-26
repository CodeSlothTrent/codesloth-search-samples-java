package HalfFloatDemo;

import HalfFloatDemo.Documents.ProductDocument;
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
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for half_float field indexing in OpenSearch.
 * Half_float fields store 16-bit IEEE 754 floating point values.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class HalfFloatIndexingTests {
    private static final Logger logger = LogManager.getLogger(HalfFloatIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public HalfFloatIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "19.99, 'Typical price value is indexed correctly'",
            "0.5, 'Small decimal value is indexed correctly'",
            "-10.5, 'Negative value is indexed correctly'"
    })
    public void halfFloatMapping_IndexesNumericValueCorrectly(float priceValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.halfFloat(h -> h))))) {

            ProductDocument productDocument = new ProductDocument(1, "Mouse", priceValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getPrice()).as(explanation).isCloseTo(priceValue, within(0.01f));
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }
}

