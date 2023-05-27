package db.fs.reader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import db.DBMSException;
import db.PathUtils;
import db.entries.TableContents;
import db.entries.TableInfoHolder;
import db.entries.TableMetaInfo;
import db.fs.DataPage;
import db.fs.TableSchemaSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author vlad333rrty
 */
public class DBReader {
    private static final Logger logger = LogManager.getLogger(DBReader.class);

    private final Path rootPath;
    private final DataPagesReader pagesReader;

    public DBReader(Path rootPath) {
        this.rootPath = rootPath;
        this.pagesReader = new DataPagesReader(rootPath);
    }

    public TableInfoHolder recoverTables() throws DBMSException {
        if (!Files.exists(rootPath)) {
            throw new DBMSException("FATAL! No database found");
        }
        File[] tableFiles = PathUtils.getTablePath(rootPath).toFile().listFiles();


        var tableInfoHolder = new TableInfoHolder();

        for (File tableFile : tableFiles) {
            File[] files = tableFile.listFiles();
            if (files == null) {
                logger.info("Db is empty");
                return new TableInfoHolder();
            }

            List<DataPage> pages = pagesReader.readPages(tableFile.getName());
            for (File file : files) {
                if (Pattern.matches("schema.+", file.getName())) {
                    TableMetaInfo metaInfo = TableSchemaSerializer.INSTANCE.deserialize(file.toPath());
                    tableInfoHolder.addMetaInfo(tableFile.getName(), metaInfo);
                }
            }
            tableInfoHolder.addTableContents(tableFile.getName(), new TableContents(pages));
        }

        return tableInfoHolder;
    }
}
