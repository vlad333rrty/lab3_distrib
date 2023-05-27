package db.instructions;

import db.DBMSException;

/**
 * @author vlad333rrty
 */
public class TableCreationException extends DBMSException {
    public TableCreationException(String message) {
        super(message);
    }

    public TableCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
