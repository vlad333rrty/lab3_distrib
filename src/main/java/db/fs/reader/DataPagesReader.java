package db.fs.reader;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import db.DBMSException;
import db.PathUtils;
import db.fs.DataPage;
import db.fs.DataPageSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author vlad333rrty
 */
public class DataPagesReader {


    private static final Logger logger = LogManager.getLogger(DataPagesReader.class);

    private final Path rootPath;

    public DataPagesReader(Path rootPath) {
        this.rootPath = rootPath;
    }

    public List<DataPage> readPages(String tableName) throws DBMSException {
        List<DataPage> pages = new ArrayList<>();
        File[] pageFiles = PathUtils.getTablePath(rootPath).resolve(tableName).toFile().listFiles();

        if (pageFiles == null) {
            logger.info("No dat found for table {}", tableName);
            return List.of();
        }

        for (File file : pageFiles) {
            if (isDataPageFile(file.getName())) {
                DataPage dataPage = DataPageSerializer.INSTANCE.deserialize(file.toPath());
                // todo this is a hack to deal with lock serialization issue
                pages.add(new DataPage(dataPage.number(), dataPage.getRecords()));
            }
        }

        return pages;
    }

    private boolean isDataPageFile(String fileName) {
        return Pattern.matches("page\\d+\\.pf", fileName);
    }
}
