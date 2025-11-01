package FlattenedDemo.Documents;

/**
 * Strongly-typed record representing product metadata.
 * This record structure will be serialized as a JSON object in OpenSearch.
 */
public record ProductMetadata(
        String title,
        String brand,
        String category,
        Double price,
        String description
) {
}
