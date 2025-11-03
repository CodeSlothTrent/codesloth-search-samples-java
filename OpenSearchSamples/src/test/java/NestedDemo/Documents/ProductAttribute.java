package NestedDemo.Documents;

/**
 * Strongly-typed record representing a product attribute.
 * This record structure will be serialized as a JSON object in OpenSearch.
 */
public record ProductAttribute(
        String color,
        String size
) {
}

