package NumericFieldDataTypes.ScaledFloatDemo;

import NumericFieldDataTypes.ScaledFloatDemo.Documents.ProductDocument;
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
 * Tests for scaled_float field indexing in OpenSearch.
 * Scaled_float fields require a scaling_factor parameter.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ScaledFloatIndexingTests {
    private static final Logger logger = LogManager.getLogger(ScaledFloatIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ScaledFloatIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "19.99, 'Typical price value is indexed correctly'",
            "49.95, 'Decimal value is indexed correctly'",
            "0.5, 'Small decimal value is indexed correctly'"
    })
    public void scaledFloatMapping_IndexesNumericValueCorrectly(double priceValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.scaledFloat(sf -> sf.scalingFactor(100.0)))))) {

            ProductDocument productDocument = new ProductDocument(1, "Mouse", priceValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getPrice()).as(explanation).isCloseTo(priceValue, within(0.01));
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }
}

