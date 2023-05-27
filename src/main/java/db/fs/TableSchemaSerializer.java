package db.fs;

import java.nio.file.Path;

import db.DBMSException;
import db.entries.TableMetaInfo;

/**
 * @author vlad333rrty
 */
public class TableSchemaSerializer extends SerializerBase<TableMetaInfo> {
    public static final String SCHEMA_FILE_NAME = "schema.json";
    public static final TableSchemaSerializer INSTANCE = new TableSchemaSerializer();

    public TableSchemaSerializer() {
        super(TableMetaInfo.class);
    }

    public void serialize(TableMetaInfo schema, Path path) throws DBMSException {
        super.serialize(schema, path.resolve(SCHEMA_FILE_NAME));
    }
}
