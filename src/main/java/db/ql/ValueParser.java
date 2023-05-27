package db.ql;

/**
 * @author vlad333rrty
 */
public class ValueParser {
    public static Object parse(String value, Type type) {
        return switch (type) {
            case INT -> Integer.parseInt(value);
            case FLOAT -> Float.parseFloat(value);
            case STRING -> value;
        };
    }
}
