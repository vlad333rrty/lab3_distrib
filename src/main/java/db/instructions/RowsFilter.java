package db.instructions;

import java.util.List;
import java.util.Map;

/**
 * @author vlad333rrty
 */
public class RowsFilter {
    public static boolean doesRowFitCondition(List<Object> values, Map<Integer, Object> attributeIndex2ExpectedValue) {
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            Object expectedValue = attributeIndex2ExpectedValue.get(i);
            if (expectedValue != null && !value.equals(expectedValue)) {
                return false;
            }
        }
        return true;
    }
}
