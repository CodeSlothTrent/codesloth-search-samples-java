package FlattenedDemo.Documents;

/**
 * Strongly-typed record representing a date attribute.
 * This record structure will be serialized as a JSON object in OpenSearch.
 * The date field is stored as a String to allow testing both ISO8601 and non-ISO formatted dates.
 */
public record DateAttribute(
        String createdDate
) {
}

