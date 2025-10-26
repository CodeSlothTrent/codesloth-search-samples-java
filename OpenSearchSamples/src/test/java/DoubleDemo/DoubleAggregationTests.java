package DoubleDemo;

import DoubleDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.aggregations.DoubleTermsAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for double field aggregations in OpenSearch.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class DoubleAggregationTests {
    private static final Logger logger = LogManager.getLogger(DoubleAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public DoubleAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @Test
    public void doubleMapping_CanBeUsedForTermsAggregation() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.double_(d -> d))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99),
                    new ProductDocument(2, "Keyboard", 49.95),
                    new ProductDocument(3, "Monitor", 19.99),
                    new ProductDocument(4, "Cable", 19.99),
                    new ProductDocument(5, "Headset", 49.95)
            };
            testIndex.indexDocuments(productDocuments);

            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0)
                    .aggregations("price_counts", a -> a
                            .terms(t -> t
                                    .field("price")
                                    .size(10)
                            )
                    )
                    .build();

            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            assertThat(response.aggregations()).isNotNull();
            DoubleTermsAggregate termsAgg = response.aggregations().get("price_counts").dterms();
            assertThat(termsAgg.buckets().array()).hasSize(2);
        }
    }

    @Test
    public void doubleMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.double_(d -> d))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99),
                    new ProductDocument(2, "Keyboard", 49.95),
                    new ProductDocument(3, "Monitor", 19.99)
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

