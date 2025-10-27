package NumericFieldDataTypes.DoubleDemo;

import NumericFieldDataTypes.DoubleDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for double field searching in OpenSearch.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class DoubleSearchingTests {
    private static final Logger logger = LogManager.getLogger(DoubleSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public DoubleSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "19.99, 'Matches documents with exact price value of 19.99'",
            "99.99, 'Matches documents with exact price value of 99.99'"
    })
    public void doubleMapping_ExactlyMatchesTermQuery(double priceValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.double_(d -> d))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99),
                    new ProductDocument(2, "Keyboard", 49.95),
                    new ProductDocument(3, "Monitor", 99.99)
            };
            testIndex.indexDocuments(productDocuments);

            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("price")
                                            .value(FieldValue.of(priceValue))
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "0.0, 50.0, 2, 'Range [0.0,50.0] matches 2 documents'",
            "50.0, 200.0, 1, 'Range [50.0,200.0] matches 1 document'"
    })
    public void doubleMapping_SupportsRangeQueries(double gte, double lte, long expectedHits, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.double_(d -> d))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99),
                    new ProductDocument(2, "Keyboard", 49.95),
                    new ProductDocument(3, "Monitor", 99.99)
            };
            testIndex.indexDocuments(productDocuments);

            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("price")
                                            .gte(JsonData.of(gte))
                                            .lte(JsonData.of(lte))
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).as(explanation).isEqualTo(expectedHits);
            assertThat(result.hits().hits()).hasSize((int) expectedHits);
        }
    }
}

