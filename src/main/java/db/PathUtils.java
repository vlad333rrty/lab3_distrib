package db;

import java.nio.file.Path;

import db.fs.TableSchemaSerializer;

/**
 * @author vlad333rrty
 *
 * todo is it ok?
 */
public final class PathUtils {
    public static Path getTablePath(Path dbPath) {
        return dbPath.resolve("tables");
    }

    public static Path getSchemaPath(Path tablePath) {
        return tablePath.resolve(TableSchemaSerializer.SCHEMA_FILE_NAME);
    }
}
