package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains multiple flattened fields (category and manufacturer).
 * This demonstrates that you can match multiple values from different flattened fields
 * in the same document using boolean must queries.
 * 
 * Uses strongly-typed record types for type safety at compile time.
 */
public class ProductWithMultipleFlattenedFields implements IDocumentWithId {
    private String id;
    private String name;
    private CategoryInfo category; // Flattened field for category information (strongly-typed record)
    private ManufacturerInfo manufacturer; // Flattened field for manufacturer information (strongly-typed record)

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithMultipleFlattenedFields() {
    }

    /**
     * Creates a new ProductWithMultipleFlattenedFields with the specified ID, name, category, and manufacturer.
     *
     * @param id            The product ID
     * @param name          The product name
     * @param category      The product category information as a record (will be serialized as JSON object)
     * @param manufacturer  The product manufacturer information as a record (will be serialized as JSON object)
     */
    public ProductWithMultipleFlattenedFields(String id, String name, CategoryInfo category, ManufacturerInfo manufacturer) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.category = category;
        this.manufacturer = manufacturer;
    }

    /**
     * Gets the product ID.
     *
     * @return The product ID
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Sets the product ID.
     *
     * @param id The product ID
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the product name.
     *
     * @return The product name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the product name.
     *
     * @param name The product name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the product category information.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: category.name, category.level, etc.
     *
     * @return The product category as a record
     */
    public CategoryInfo getCategory() {
        return category;
    }

    /**
     * Sets the product category.
     *
     * @param category The product category as a record
     */
    public void setCategory(CategoryInfo category) {
        this.category = category;
    }

    /**
     * Gets the product manufacturer information.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: manufacturer.name, manufacturer.country, etc.
     *
     * @return The product manufacturer as a record
     */
    public ManufacturerInfo getManufacturer() {
        return manufacturer;
    }

    /**
     * Sets the product manufacturer.
     *
     * @param manufacturer The product manufacturer as a record
     */
    public void setManufacturer(ManufacturerInfo manufacturer) {
        this.manufacturer = manufacturer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithMultipleFlattenedFields that = (ProductWithMultipleFlattenedFields) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(category, that.category) &&
                Objects.equals(manufacturer, that.manufacturer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, category, manufacturer);
    }

    @Override
    public String toString() {
        return "ProductWithMultipleFlattenedFields{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", category=" + category +
                ", manufacturer=" + manufacturer +
                '}';
    }
}

