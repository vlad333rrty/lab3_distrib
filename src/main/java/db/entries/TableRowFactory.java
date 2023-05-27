package db.entries;

import java.util.List;
import java.util.UUID;

/**
 * @author vlad333rrty
 */
public class TableRowFactory {
    public static TableRow createTableRow(List<Object> values) {
        return new TableRow(generateFQDN(), values);
    }

    private static String generateFQDN() {
        return UUID.randomUUID().toString();
    }
}
