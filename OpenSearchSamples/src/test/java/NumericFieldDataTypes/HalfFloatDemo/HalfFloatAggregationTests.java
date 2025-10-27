package NumericFieldDataTypes.HalfFloatDemo;

import NumericFieldDataTypes.HalfFloatDemo.Documents.ProductDocument;
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

@ExtendWith(OpenSearchResourceManagementExtension.class)
public class HalfFloatAggregationTests {
    private static final Logger logger = LogManager.getLogger(HalfFloatAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public HalfFloatAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @Test
    public void halfFloatMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.halfFloat(h -> h))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 19.99f)
            };
            testIndex.indexDocuments(productDocuments);

            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0)
                    .aggregations("distinctPrices", a -> a
                            .cardinality(c -> c
                                    .field("price")
                            )
                    )
                    .build();

            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            assertThat(response.aggregations()).isNotNull();
            CardinalityAggregate cardinalityAgg = response.aggregations().get("distinctPrices").cardinality();
            assertThat(cardinalityAgg.value()).isEqualTo(2);
        }
    }
}

