package db.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import db.DBMSException;
import db.PathUtils;
import db.entries.TableInfoHolder;
import db.entries.TableRow;
import db.fs.DataPage;
import db.fs.DataPageFSManager;
import db.instructions.Instruction;

/**
 * @author vlad333rrty
 */
public class UncommittedTransactionsSolver {
    private final TableInfoHolder tableInfoHolder;
    private final TransactionLogManager transactionLogManager;

    public UncommittedTransactionsSolver(TableInfoHolder tableInfoHolder, TransactionLogManager transactionLogManager) {
        this.tableInfoHolder = tableInfoHolder;
        this.transactionLogManager = transactionLogManager;
    }

    public void rollbackUncommittedTransactions(Path dbPath, Path transactionLogPath) throws DBMSException {
        rollbackTransactionsAndDump(dbPath, transactionLogPath, x -> true);
    }

    public void rollbackUncommittedTransaction(Path dbPath, Path transactionLogPath, String transactionId) throws DBMSException {
        rollbackTransactionsAndDump(dbPath, transactionLogPath, x -> x.equals(transactionId));
    }

    private void rollbackTransactionsAndDump(
            Path dbPath,
            Path transactionLogPath,
            Predicate<String> filter) throws DBMSException
    {
        if (!Files.exists(transactionLogPath)) {
            return;
        }
        Map<String, List<DataPage>> tableName2ModifiedPages = new HashMap<>();
        Set<String> canceledTransactions = new HashSet<>();
        try (Stream<Path> logs = Files.list(transactionLogPath)) {
            logs.flatMap(path -> {
                try {
                    return TransactionLogEntrySerializer.INSTANCE.deserialize(path).stream();
                } catch (DBMSException e) {
                    throw new RuntimeException(e);
                }
            }).filter(logEntry -> filter.test(logEntry.getTransactionId()))
                    .forEach(logEntry -> performTransactionRollback(logEntry, tableName2ModifiedPages, canceledTransactions));
        } catch (IOException e) {
            throw new DBMSException("Failed to rollback uncommitted changes", e);
        }

        try {
            for (String id : canceledTransactions) {
                transactionLogManager.removeLog(id);
            }
        } catch (IOException e) {
            throw new DBMSException("Failed to cancel transaction", e);
        }

        for (var entry : tableName2ModifiedPages.entrySet()) {
            DataPageFSManager.dump(PathUtils.getTablePath(dbPath).resolve(entry.getKey()), entry.getValue());
        }
    }

    private void performTransactionRollback(
            TransactionLogEntry logEntry,
            Map<String, List<DataPage>> tableName2ModifiedPages,
            Set<String> canceledTransactions)
    {
        List<DataPage> pages = tableInfoHolder.getTableContents(logEntry.getTableName()).getPages();
        int pageId =  Integer.parseInt(logEntry.getPageId());
        DataPage modifiedPage = pages.stream()
                .filter(page -> page.number() == pageId)
                .findFirst()
                .orElseThrow();
        List<TableRow> rows = modifiedPage.getRecords();
        if (logEntry.getInstructionType().equals(Instruction.Type.DELETE.name())) {
            rows.add(logEntry.getBeforeValue());
        } else if (logEntry.getInstructionType().equals(Instruction.Type.INSERT.name())) {
            String rowFqdn = logEntry.getAfterValue().fqdn();
            int pos = IntStream.range(0, rows.size())
                    .filter(i -> rows.get(i).fqdn().equals(rowFqdn))
                    .findFirst()
                    .orElseThrow();
            rows.remove(pos); // todo
        } else if (logEntry.getInstructionType().equals(Instruction.Type.UPDATE.name())) {
            String rowFqdn = logEntry.getAfterValue().fqdn();
            int pos = IntStream.range(0, rows.size())
                    .filter(i -> rows.get(i).fqdn().equals(rowFqdn))
                    .findFirst()
                    .orElseThrow();
            rows.set(pos, logEntry.getBeforeValue());
        }
        tableName2ModifiedPages.computeIfAbsent(logEntry.getTableName(), k -> new ArrayList<>()).add(modifiedPage);
        canceledTransactions.add(logEntry.getTransactionId());
    }
}
