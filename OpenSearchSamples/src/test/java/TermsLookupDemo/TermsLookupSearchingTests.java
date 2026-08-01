package TermsLookupDemo;

import TermsLookupDemo.Documents.ProductDocument;
import TermsLookupDemo.Documents.UserPersonalizationDocument;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests demonstrating terms lookup queries across two indexes.
 * <p>
 * Terms lookup retrieves an array of values from a document in one index (user personalization)
 * and uses them as search terms against another index (product catalog). This enables personalized
 * search results without requiring the application to perform two separate queries.
 * <p>
 * OpenSearch documentation: https://opensearch.org/docs/latest/query-dsl/term/terms/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class TermsLookupSearchingTests {
    private static final Logger logger = LogManager.getLogger(TermsLookupSearchingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public TermsLookupSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    private OpenSearchTestIndex createProductIndex() throws IOException {
        return fixture.createTestIndex(mapping -> mapping
                .properties("title", Property.of(p -> p.keyword(k -> k)))
                .properties("price", Property.of(p -> p.double_(d -> d)))
                .properties("description", Property.of(p -> p.text(t -> t)))
        );
    }

    private OpenSearchTestIndex createUserPersonalizationIndex() throws IOException {
        return fixture.createTestIndex(mapping -> mapping
                .properties("favoritedProductIds", Property.of(p -> p.keyword(k -> k)))
                .properties("recentlyViewedProductIds", Property.of(p -> p.keyword(k -> k)))
                .properties("inCartProductIds", Property.of(p -> p.keyword(k -> k)))
        );
    }

    private ProductDocument[] getSampleProducts() {
        return new ProductDocument[]{
                new ProductDocument(1, "Wireless Mouse", 29.99, "Ergonomic wireless mouse with USB receiver"),
                new ProductDocument(2, "Mechanical Keyboard", 89.99, "RGB mechanical keyboard with Cherry MX switches"),
                new ProductDocument(3, "USB-C Hub", 45.99, "7-in-1 USB-C hub with HDMI and Ethernet"),
                new ProductDocument(4, "Monitor Stand", 34.99, "Adjustable monitor stand with storage drawer"),
                new ProductDocument(5, "HD Webcam", 59.99, "1080p HD webcam with built-in microphone")
        };
    }

    private UserPersonalizationDocument[] getSampleUsers() {
        return new UserPersonalizationDocument[]{
                new UserPersonalizationDocument("user-1",
                        new String[]{"1", "3", "5"},
                        new String[]{"1", "2", "3", "4"},
                        new String[]{"1", "3"}
                ),
                new UserPersonalizationDocument("user-2",
                        new String[]{"2", "4"},
                        new String[]{"2", "5"},
                        new String[]{"2"}
                )
        };
    }

    /**
     * A terms lookup query retrieves the favoritedProductIds array from user-1's document
     * in the user personalization index and matches those IDs against product _id values.
     */
    @Test
    public void termsLookup_FindsFavoritedProductsForUser() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .terms(t -> t
                                            .field("_id")
                                            .terms(terms -> terms
                                                    .lookup(l -> l
                                                            .index(userIndex.getName())
                                                            .id("user-1")
                                                            .path("favoritedProductIds")
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(3);

            List<String> matchedTitles = result.hits().hits().stream()
                    .map(hit -> hit.source().getTitle())
                    .toList();
            assertThat(matchedTitles).containsExactlyInAnyOrder("Wireless Mouse", "USB-C Hub", "HD Webcam");
        }
    }

    /**
     * Demonstrates terms lookup on a different personalization path (recentlyViewedProductIds),
     * returning a broader set of products the user has interacted with.
     */
    @Test
    public void termsLookup_FindsRecentlyViewedProductsForUser() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .terms(t -> t
                                            .field("_id")
                                            .terms(terms -> terms
                                                    .lookup(l -> l
                                                            .index(userIndex.getName())
                                                            .id("user-1")
                                                            .path("recentlyViewedProductIds")
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(4);

            List<String> matchedTitles = result.hits().hits().stream()
                    .map(hit -> hit.source().getTitle())
                    .toList();
            assertThat(matchedTitles).containsExactlyInAnyOrder(
                    "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub", "Monitor Stand");
        }
    }

    /**
     * Demonstrates terms lookup on the inCartProductIds path, returning only the
     * products the user has added to their shopping cart.
     */
    @Test
    public void termsLookup_FindsInCartProductsForUser() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .terms(t -> t
                                            .field("_id")
                                            .terms(terms -> terms
                                                    .lookup(l -> l
                                                            .index(userIndex.getName())
                                                            .id("user-1")
                                                            .path("inCartProductIds")
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(2);

            List<String> matchedTitles = result.hits().hits().stream()
                    .map(hit -> hit.source().getTitle())
                    .toList();
            assertThat(matchedTitles).containsExactlyInAnyOrder("Wireless Mouse", "USB-C Hub");
        }
    }

    /**
     * The same terms lookup structure produces different results per user because each user
     * document contains a different set of product IDs in their personalization arrays.
     */
    @Test
    public void termsLookup_ReturnsDifferentResultsPerUser() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .terms(t -> t
                                            .field("_id")
                                            .terms(terms -> terms
                                                    .lookup(l -> l
                                                            .index(userIndex.getName())
                                                            .id("user-2")
                                                            .path("favoritedProductIds")
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(2);

            List<String> matchedTitles = result.hits().hits().stream()
                    .map(hit -> hit.source().getTitle())
                    .toList();
            assertThat(matchedTitles).containsExactlyInAnyOrder("Mechanical Keyboard", "Monitor Stand");
        }
    }

    /**
     * Combines a terms lookup with a price range filter inside a bool query.
     * User-1's favorited products are [1, 3, 5] (Wireless Mouse $29.99, USB-C Hub $45.99, HD Webcam $59.99).
     * Filtering to price <= 50.00 narrows this to Wireless Mouse and USB-C Hub.
     */
    @Test
    public void termsLookup_CombinedWithPriceFilter_FindsFavoritedProductsUnderPrice() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(f -> f
                                                    .terms(t -> t
                                                            .field("_id")
                                                            .terms(terms -> terms
                                                                    .lookup(l -> l
                                                                            .index(userIndex.getName())
                                                                            .id("user-1")
                                                                            .path("favoritedProductIds")
                                                                    )
                                                            )
                                                    )
                                            )
                                            .filter(f -> f
                                                    .range(r -> r
                                                            .field("price")
                                                            .lte(JsonData.of(50.00))
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(2);

            List<String> matchedTitles = result.hits().hits().stream()
                    .map(hit -> hit.source().getTitle())
                    .toList();
            assertThat(matchedTitles).containsExactlyInAnyOrder("Wireless Mouse", "USB-C Hub");
        }
    }

    /**
     * Combines a terms lookup with a full-text match query on the description field.
     * User-1's recently viewed products are [1, 2, 3, 4]. Searching for "keyboard"
     * in the description narrows results to the Mechanical Keyboard (product 2),
     * whose description contains "mechanical keyboard with Cherry MX switches".
     */
    @Test
    public void termsLookup_CombinedWithTextSearch_FindsViewedProductsMatchingDescription() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());
            userIndex.indexDocuments(getSampleUsers());

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(f -> f
                                                    .terms(t -> t
                                                            .field("_id")
                                                            .terms(terms -> terms
                                                                    .lookup(l -> l
                                                                            .index(userIndex.getName())
                                                                            .id("user-1")
                                                                            .path("recentlyViewedProductIds")
                                                                    )
                                                            )
                                                    )
                                            )
                                            .must(m -> m
                                                    .match(mt -> mt
                                                            .field("description")
                                                            .query(FieldValue.of("keyboard"))
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().get(0).source().getTitle()).isEqualTo("Mechanical Keyboard");
        }
    }

    /**
     * When a user's personalization array is empty, the terms lookup produces no matching terms,
     * so the query returns zero results.
     */
    @Test
    public void termsLookup_ReturnsNoResults_WhenUserHasNoInteractions() throws Exception {
        try (OpenSearchTestIndex productIndex = createProductIndex();
             OpenSearchTestIndex userIndex = createUserPersonalizationIndex()) {

            productIndex.indexDocuments(getSampleProducts());

            UserPersonalizationDocument[] users = new UserPersonalizationDocument[]{
                    new UserPersonalizationDocument("user-empty",
                            new String[]{},
                            new String[]{},
                            new String[]{})
            };
            userIndex.indexDocuments(users);

            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(productIndex.getName())
                            .query(q -> q
                                    .terms(t -> t
                                            .field("_id")
                                            .terms(terms -> terms
                                                    .lookup(l -> l
                                                            .index(userIndex.getName())
                                                            .id("user-empty")
                                                            .path("favoritedProductIds")
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            assertThat(result.hits().total().value()).isEqualTo(0);
            assertThat(result.hits().hits()).isEmpty();
        }
    }
}
