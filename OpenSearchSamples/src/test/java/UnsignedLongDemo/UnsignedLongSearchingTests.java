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
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for unsigned_long field searching in OpenSearch.
 * 
 * Note: unsigned_long field type was introduced in OpenSearch 2.4.0
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class UnsignedLongSearchingTests {
    private static final Logger logger = LogManager.getLogger(UnsignedLongSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public UnsignedLongSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "1000, 'Matches documents with exact stock value of 1000'",
            "5000, 'Matches documents with exact stock value of 5000'"
    })
    public void unsignedLongMapping_ExactlyMatchesTermQuery(long stockValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.unsignedLong(ul -> ul))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000L),
                    new ProductDocument(2, "Keyboard", 5000L),
                    new ProductDocument(3, "Monitor", 10000L)
            };
            testIndex.indexDocuments(productDocuments);

            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("stock")
                                            .value(FieldValue.of(stockValue))
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
        }
    }
}

