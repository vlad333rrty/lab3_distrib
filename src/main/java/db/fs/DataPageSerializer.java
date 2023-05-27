package db.fs;



/**
 * @author vlad333rrty
 */
public class DataPageSerializer extends SerializerBase<DataPage> {
    public static final DataPageSerializer INSTANCE = new DataPageSerializer();

    public DataPageSerializer() {
        super(DataPage.class);
    }
}
