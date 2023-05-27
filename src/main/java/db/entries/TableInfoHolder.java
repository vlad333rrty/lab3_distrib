package db.entries;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author vlad333rrty
 */
public class TableInfoHolder {
    private final Map<String, TableMetaInfo> tableName2MetaInfo = new ConcurrentHashMap<>();
    private final Map<String, TableContents> tableName2Data = new ConcurrentHashMap<>();

    public void addMetaInfo(String name, TableMetaInfo metaInfo) {
        tableName2MetaInfo.put(name, metaInfo);
    }

    public void addTableContents(String name, TableContents tableData) {
        tableName2Data.put(name, tableData);
    }

    public TableMetaInfo getMetaInfo(String tableName) {
        return tableName2MetaInfo.get(tableName);
    }

    public TableContents getTableContents(String tableName) {
        return tableName2Data.get(tableName);
    }

    public Set<String> getAllTableNames() {
        return tableName2MetaInfo.keySet();
    }
}
