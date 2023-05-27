package db;

/**
 * @author vlad333rrty
 */
public class DBMSException extends Exception {
    public DBMSException(String message) {
        super(message);
    }

    public DBMSException(String message, Throwable cause) {
        super(message, cause);
    }
}
