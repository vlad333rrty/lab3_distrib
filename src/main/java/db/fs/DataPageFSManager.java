package db.fs;


import java.nio.file.Path;
import java.util.Collection;

import db.DBMSException;

/**
 * @author vlad333rrty
 */
public final class DataPageFSManager {
    private static String getFileName(DataPage dataPage) {
        return "page%s.pf".formatted(dataPage.number());
    }

    public static void dump(Path tablePath, Collection<DataPage> pages) throws DBMSException {
        for (DataPage page : pages) {
            DataPageSerializer.INSTANCE.serialize(page, tablePath.resolve(getFileName(page)));
        }
    }
}
