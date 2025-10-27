package NumericFieldDataTypes.UnsignedLongDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains an unsigned_long field for testing unsigned_long field type functionality.
 * Unsigned_long is an unsigned 64-bit integer (0 to 18,446,744,073,709,551,615), represented as long in Java.
 * 
 * Note: unsigned_long field type was introduced in OpenSearch 2.4.0
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private long stock;

    public ProductDocument() {
    }

    public ProductDocument(int id, String name, long stock) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.stock = stock;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getStock() {
        return stock;
    }

    public void setStock(long stock) {
        this.stock = stock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return stock == that.stock &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, stock);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", stock=" + stock +
                '}';
    }
}

