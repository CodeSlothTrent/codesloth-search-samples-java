package FlattenedDemo.Documents;

/**
 * A strongly-typed record representing product manufacturer information.
 * Used to demonstrate multiple flattened fields in the same document.
 */
public record ManufacturerInfo(
        String name,
        String country
) {
}
