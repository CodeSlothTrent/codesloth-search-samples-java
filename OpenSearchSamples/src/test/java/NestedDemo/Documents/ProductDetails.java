package NestedDemo.Documents;

/**
 * Strongly-typed record representing product details that contains nested attributes.
 * This demonstrates nested nested field structures where a nested field contains
 * another complex type.
 */
public record ProductDetails(
        ProductAttribute attribute, // Nested ProductAttribute (color, size)
        String description
) {
}

