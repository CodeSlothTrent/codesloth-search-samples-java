package HalfFloatDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a half_float field for testing half_float field type functionality.
 * Half_float is a 16-bit IEEE 754 floating point, represented as float in Java.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private float price;

    public ProductDocument() {
    }

    public ProductDocument(int id, String name, float price) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.price = price;
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

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return Float.compare(that.price, price) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, price);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}

