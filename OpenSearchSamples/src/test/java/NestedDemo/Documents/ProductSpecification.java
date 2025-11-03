package NestedDemo.Documents;

/**
 * A record representing product specification information.
 * This is used to demonstrate that nested fields can use different DTO types
 * within the same document - they don't all need to use the same record structure.
 */
public record ProductSpecification(
        String brand,
        String category
) {
}

