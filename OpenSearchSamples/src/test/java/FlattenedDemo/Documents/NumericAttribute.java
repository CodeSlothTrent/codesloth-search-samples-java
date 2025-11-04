package FlattenedDemo.Documents;

/**
 * Strongly-typed record representing a numeric attribute.
 * This record structure will be serialized as a JSON object in OpenSearch.
 * The numeric field is stored as a String to allow testing both padded and unpadded numeric values.
 * Since flattened fields store values as keywords, numeric values are compared lexicographically (ASCII),
 * not numerically. This requires zero-padding for range queries to work correctly.
 */
public record NumericAttribute(
        String value
) {
}

