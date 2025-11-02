package FlattenedDemo.Documents;

/**
 * Strongly-typed record representing product details that contains nested attributes.
 * This demonstrates nested flattened field structures where a flattened field contains
 * another complex type, requiring multiple dots to access leaf properties.
 */
public record ProductDetails(
        ProductAttribute attribute, // Nested ProductAttribute (color, size)
        String description
) {
}

