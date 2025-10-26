package ScaledFloatDemo;

import ScaledFloatDemo.Documents.ProductDocument;
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

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ScaledFloatSearchingTests {
    private static final Logger logger = LogManager.getLogger(ScaledFloatSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ScaledFloatSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "19.99, 'Matches documents with exact price value of 19.99'",
            "49.95, 'Matches documents with exact price value of 49.95'"
    })
    public void scaledFloatMapping_ExactlyMatchesTermQuery(double priceValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.scaledFloat(sf -> sf.scalingFactor(100.0)))))) {

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
}

