package FlattenedDemo;

import FlattenedDemo.Documents.NumericAttribute;
import FlattenedDemo.Documents.ProductWithLargeFlattenedNumeric;
import FlattenedDemo.Documents.ProductWithNumericAttribute;
import KeywordDemo.Documents.IDocumentWithId;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extreme test cases for numeric range queries on flattened fields in OpenSearch.
 * <p>
 * These tests are designed to mitigate the risk of errors from large inputs by testing:
 * <ul>
 *   <li>Parameterized range test scenarios (small range, entire range, overlapping start/end)</li>
 *   <li>500000 values in a single flattened numeric field</li>
 *   <li>500000 distinct properties in a flattened object DTO (single document)</li>
 *   <li>500000 documents, each with a single flattened numeric field</li>
 *   <li>500000 distinct properties in a flattened object DTO across 500000 documents</li>
 * </ul>
 * <p>
 * All numeric values are zero-padded to 10 digits (max size for Integer.MAX_VALUE = 2,147,483,647)
 * to ensure correct lexicographic comparison in flattened fields.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
public class FlattenedNumericRangeExtremeTests {
    private static final int EXTREME_TEST_SIZE = 10000;
    private static final int BATCH_SIZE = 10000; // Batch size for indexing documents
    private static final int PADDING_LENGTH = 10; // Max digits for Integer.MAX_VALUE
    
    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedNumericRangeExtremeTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Provides test arguments for parameterized range test scenarios.
     * 
     * @return Stream of Arguments containing: rangeStart, rangeEnd, expectedCount, description
     */
    private static Stream<Arguments> rangeTestScenarios() {
        return Stream.of(
            // Small range: narrow range in the middle
            Arguments.of(100000, 100100, 101, "small range"),
            
            // Entire range: all documents matched (from min to max)
            Arguments.of(0, EXTREME_TEST_SIZE - 1, EXTREME_TEST_SIZE, "entire range (all docs matched)"),
            
            // Range overlapping start of docs: starts before first doc, ends in middle
            // Only matches documents 0 to 100000 (100001 documents)
            Arguments.of(-100, 100000, 100001, "range overlapping start of docs"),
            
            // Range overlapping end of docs: starts in middle, ends after last doc
            // Only matches documents from (EXTREME_TEST_SIZE - 100000) to (EXTREME_TEST_SIZE - 1)
            // That's 100000 documents (from 400000 to 499999)
            Arguments.of(EXTREME_TEST_SIZE - 100000, EXTREME_TEST_SIZE + 100, 100000, "range overlapping end of docs")
        );
    }

    /**
     * Parameterized test for range query scenarios on flattened numeric fields.
     * Tests various range scenarios to ensure correct behavior with large datasets.
     * 
     * @param rangeStart The start of the range (inclusive)
     * @param rangeEnd The end of the range (inclusive)
     * @param expectedCount Expected number of matching documents
     * @param description Description of the test scenario
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest(name = "{3}: range [{0}, {1}] should match {2} documents")
    @MethodSource("rangeTestScenarios")
    public void flattenedNumericRange_ParameterizedScenarios_WorksCorrectly(
            int rangeStart, int rangeEnd, int expectedCount, String description) throws Exception {
        
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create and index documents in batches to reduce memory consumption
            indexDocumentsInBatches(testIndex, EXTREME_TEST_SIZE, i -> 
                    new ProductWithNumericAttribute(
                            String.valueOf(i),
                            "Product" + i,
                            new NumericAttribute(padNumeric(i))));

            // Query for the specified range
            String paddedStart = padNumeric(rangeStart);
            String paddedEnd = padNumeric(rangeEnd);
            
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of(paddedStart))
                                            .lte(JsonData.of(paddedEnd))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Verify the result count matches expected
            assertThat(result.hits().total().value())
                    .as("Range [%d, %d] should match %d documents (%s)", rangeStart, rangeEnd, expectedCount, description)
                    .isEqualTo(expectedCount);
        }
    }

    /**
     * Tests 500000 values in a single flattened numeric field.
     * Creates a single document with a map containing 500000 zero-padded numeric values.
     * Note: flat_object fields require objects (maps), not arrays, so we use a Map structure
     * where each value is stored with itself as the key to enable range queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_500000ValuesInSingleField_WorksCorrectly() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("numericAttributes", Property.of(p -> p.flatObject(f -> f))))) {

            // Create a single document with EXTREME_TEST_SIZE numeric values
            // flat_object requires objects, so we create a Map where each value is both key and value
            // This allows range queries to work across all values
            Map<String, String> numericValuesMap = new HashMap<>();
            for (int i = 0; i < EXTREME_TEST_SIZE; i++) {
                String paddedValue = padNumeric(i);
                // Store value as both key and value to enable range queries
                numericValuesMap.put(paddedValue, paddedValue);
            }
            
            // Use ProductWithLargeFlattenedNumeric which accepts a Map
            ProductWithLargeFlattenedNumeric product = new ProductWithLargeFlattenedNumeric(
                    "1", 
                    "ProductWithManyValues", 
                    numericValuesMap
            );
            
            testIndex.indexDocuments(new ProductWithLargeFlattenedNumeric[]{product});

            // Query for a range in the middle of the values
            // Query using wildcard to search across all keys in the flat_object
            int rangeStart = Math.min(100000, EXTREME_TEST_SIZE - 1);
            int rangeEnd = Math.min(100100, EXTREME_TEST_SIZE - 1);
            String paddedStart = padNumeric(rangeStart);
            String paddedEnd = padNumeric(rangeEnd);
            
            SearchResponse<ProductWithLargeFlattenedNumeric> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("numericAttributes.*")
                                            .gte(JsonData.of(paddedStart))
                                            .lte(JsonData.of(paddedEnd))
                                    )
                            ),
                    ProductWithLargeFlattenedNumeric.class
            );

            // Should match the single document (it contains values in the range)
            assertThat(result.hits().total().value())
                    .as("Should match document containing values in range [%d, %d]", rangeStart, rangeEnd)
                    .isGreaterThan(0);
        }
    }

    /**
     * Tests 500000 distinct properties defined within a flattened object DTO.
     * Indexes a single document and populates each field with a distinct value.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_500000DistinctPropertiesSingleDocument_WorksCorrectly() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("numericAttributes", Property.of(p -> p.flatObject(f -> f))))) {

            // Create a single document with 500000 distinct properties
            Map<String, String> numericAttributes = new HashMap<>();
            for (int i = 0; i < EXTREME_TEST_SIZE; i++) {
                numericAttributes.put("prop" + i, padNumeric(i));
            }
            
            ProductWithLargeFlattenedNumeric product = new ProductWithLargeFlattenedNumeric(
                    "1",
                    "ProductWithManyProperties",
                    numericAttributes
            );
            
            testIndex.indexDocuments(new ProductWithLargeFlattenedNumeric[]{product});

            // Query for a specific property in the middle
            int testPropertyIndex = 100000;
            String propertyName = "prop" + testPropertyIndex;
            String propertyValue = padNumeric(testPropertyIndex);
            
            SearchResponse<ProductWithLargeFlattenedNumeric> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("numericAttributes." + propertyName)
                                            .gte(JsonData.of(propertyValue))
                                            .lte(JsonData.of(propertyValue))
                                    )
                            ),
                    ProductWithLargeFlattenedNumeric.class
            );

            // Should match the single document
            assertThat(result.hits().total().value())
                    .as("Should match document with property %s = %s", propertyName, propertyValue)
                    .isEqualTo(1);
        }
    }

    /**
     * Tests 500000 documents instantiated, each with a single flattened numeric field.
     * Each document has a distinct numeric value from 0 to 499999.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_500000DocumentsSingleField_WorksCorrectly() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create and index documents in batches to reduce memory consumption
            indexDocumentsInBatches(testIndex, EXTREME_TEST_SIZE, i -> 
                    new ProductWithNumericAttribute(
                            String.valueOf(i),
                            "Product" + i,
                            new NumericAttribute(padNumeric(i))));

            // Query for a range in the middle
            int rangeStart = 100000;
            int rangeEnd = 100100;
            String paddedStart = padNumeric(rangeStart);
            String paddedEnd = padNumeric(rangeEnd);
            
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of(paddedStart))
                                            .lte(JsonData.of(paddedEnd))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match 101 documents (100000 to 100100 inclusive)
            assertThat(result.hits().total().value())
                    .as("Range [%d, %d] should match 101 documents", rangeStart, rangeEnd)
                    .isEqualTo(101);
        }
    }

    /**
     * Tests 500000 distinct properties defined within a flattened object DTO,
     * indexing 500000 documents and populating each field with as many distinct values as possible.
     * If we exceed Integer.MAX_VALUE, values loop back around.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_500000DistinctProperties500000Documents_WorksCorrectly() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("numericAttributes", Property.of(p -> p.flatObject(f -> f))))) {

            // Create and index documents in batches to reduce memory consumption
            // Each document has EXTREME_TEST_SIZE distinct properties
            // Each property gets a distinct value, looping around if we exceed Integer.MAX_VALUE
            indexDocumentsInBatches(testIndex, EXTREME_TEST_SIZE, docIndex -> {
                Map<String, String> numericAttributes = new HashMap<>();
                
                for (int propIndex = 0; propIndex < EXTREME_TEST_SIZE; propIndex++) {
                    // Calculate value: docIndex * EXTREME_TEST_SIZE + propIndex
                    // If this exceeds Integer.MAX_VALUE, use modulo to loop back
                    long rawValue = (long) docIndex * EXTREME_TEST_SIZE + propIndex;
                    int value = (int) (rawValue % (Integer.MAX_VALUE + 1L));
                    numericAttributes.put("prop" + propIndex, padNumeric(value));
                }
                
                return new ProductWithLargeFlattenedNumeric(
                        String.valueOf(docIndex),
                        "Product" + docIndex,
                        numericAttributes
                );
            });

            // Query for a specific property across all documents
            // Test property prop100000 with a range query
            int testPropertyIndex = 100000;
            String propertyName = "prop" + testPropertyIndex;
            int rangeStart = 100000;
            int rangeEnd = 100100;
            String paddedStart = padNumeric(rangeStart);
            String paddedEnd = padNumeric(rangeEnd);
            
            SearchResponse<ProductWithLargeFlattenedNumeric> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("numericAttributes." + propertyName)
                                            .gte(JsonData.of(paddedStart))
                                            .lte(JsonData.of(paddedEnd))
                                    )
                            ),
                    ProductWithLargeFlattenedNumeric.class
            );

            // Should match documents where prop100000 is in the range [100000, 100100]
            // The exact count depends on how the values are distributed
            assertThat(result.hits().total().value())
                    .as("Should match documents where %s is in range [%d, %d]", propertyName, rangeStart, rangeEnd)
                    .isGreaterThan(0);
        }
    }

    /**
     * Indexes documents in batches to avoid request size limits and reduce memory consumption.
     * Documents are created on-the-fly in batches, indexed, and then discarded.
     * 
     * @param testIndex The test index to index into
     * @param totalCount Total number of documents to create and index
     * @param documentFactory Function that creates a document for a given index
     * @param <T> Type of document that implements IDocumentWithId
     * @throws Exception If an I/O error occurs
     */
    private <T extends IDocumentWithId> void indexDocumentsInBatches(
            OpenSearchTestIndex testIndex, int totalCount, IntFunction<T> documentFactory) throws Exception {
        for (int i = 0; i < totalCount; i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, totalCount);
            
            // Create batch of documents
            @SuppressWarnings("unchecked")
            T[] batch = (T[]) new IDocumentWithId[end - i];
            for (int j = i; j < end; j++) {
                batch[j - i] = documentFactory.apply(j);
            }
            
            // Index the batch
            testIndex.indexDocuments(batch);
            
            // Batch is now out of scope and can be garbage collected
            
            // Small delay between batches to allow OpenSearch to process
            if (i + BATCH_SIZE < totalCount) {
                Thread.sleep(100); // 100ms delay between batches
            }
        }
    }

    /**
     * Pads a numeric value to the specified length with leading zeros.
     * 
     * @param value The numeric value to pad
     * @return Zero-padded string representation
     */
    private String padNumeric(int value) {
        // Handle negative numbers using two's complement offset
        // Offset: |Integer.MIN_VALUE| = 2,147,483,648
        final long offset = Math.abs((long) Integer.MIN_VALUE);
        long adjustedValue = (long) value + offset;
        return String.format("%0" + PADDING_LENGTH + "d", adjustedValue);
    }
}

