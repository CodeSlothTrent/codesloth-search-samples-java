package FlattenedDemo.Documents;

/**
 * A strongly-typed record representing product category information.
 * Used to demonstrate multiple flattened fields in the same document.
 */
public record CategoryInfo(
        String name,
        String level
) {
}
