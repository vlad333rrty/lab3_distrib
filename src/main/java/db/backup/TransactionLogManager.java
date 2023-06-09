package db.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import db.DBMSException;
import db.transaction.Transaction;

/**
 * @author vlad333rrty
 */
public class TransactionLogManager {
    private final Path transactionLogPath;


    public TransactionLogManager(Path transactionLogPath) {
        this.transactionLogPath = transactionLogPath;
    }

    public void log(List<TransactionLogEntry> logEntry, String transactionId) throws DBMSException {
        TransactionLogEntrySerializer.INSTANCE.serialize(logEntry, transactionLogPath.resolve(transactionId + ".json"));
    }

    public void commit(String transactionId) throws IOException {
        Path transactionLog = transactionLogPath.resolve(transactionId + ".json");
        Files.deleteIfExists(transactionLog);
    }

    public boolean canCommit(Transaction transaction) {
        Path transactionLog = transactionLogPath.resolve(transaction.id() + ".json");
        return Files.exists(transactionLog);
    }

    public void removeLog(String transactionId) throws IOException {
        Files.delete(transactionLogPath.resolve(transactionId + ".json"));
    }
}
