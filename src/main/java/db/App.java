package db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;

import db.backup.TransactionLogManager;
import db.backup.UncommittedTransactionsSolver;
import db.entries.TableInfoHolder;
import db.fs.DataPage;
import db.fs.DataPageFactory;
import db.fs.reader.DBReader;
import db.instructions.InstructionsExecutionManager;
import db.instructions.executors.CreateTableInstructionExecutor;
import db.instructions.executors.DeleteInstructionExecutor;
import db.instructions.executors.InsertInstructionExecutor;
import db.instructions.executors.SelectInstructionExecutor;
import db.instructions.executors.UpdateInstructionExecutor;
import db.transaction.Transaction;
import db.transaction.TransactionLocksTable;

/**
 * @author vlad333rrty
 */
public class App {
    private final DBMSController dbmsController;
    private final Consumer<String> rollbackUncommittedTransaction;

    public App(DBMSController dbmsController, Consumer<String> rollbackUncommittedTransaction) {
        this.dbmsController = dbmsController;
        this.rollbackUncommittedTransaction = rollbackUncommittedTransaction;
    }

    public static App getInstance(Path dbPath) throws DBMSException {
        return prepareConfiguration(dbPath);
    }

    public void run(Transaction transaction) throws Exception {
        dbmsController.handleTransaction(transaction);
    }

    public void commitTransaction(String transactionId) throws IOException {
        dbmsController.commitTransaction(transactionId);
    }

    public void rollbackUncommittedTransaction(String transactionId) {
        rollbackUncommittedTransaction.accept(transactionId);
    }


    private static App prepareConfiguration(Path dbPath) throws DBMSException {
        DBReader reader = new DBReader(dbPath);
        TableInfoHolder tableInfoHolder = reader.recoverTables();

        Path transactionsLogsPath = dbPath.resolve("transactions_log");
        TransactionLogManager transactionLogger = new TransactionLogManager(transactionsLogsPath);

        UncommittedTransactionsSolver uncommittedTransactionsSolver =
                new UncommittedTransactionsSolver(tableInfoHolder, transactionLogger);
        uncommittedTransactionsSolver.rollbackUncommittedTransactions(dbPath, transactionsLogsPath);

        TransactionLocksTable locksTable = new TransactionLocksTable();

        DataPageFactory dataPageFactory = new DataPageFactory(
                tableInfoHolder.getAllTableNames().stream()
                        .map(tableInfoHolder::getTableContents)
                        .flatMap(x -> x.getPages().stream())
                        .map(DataPage::number)
                        .max(Integer::compareTo)
                        .orElse(-1)
        );
        InsertInstructionExecutor insertInstructionExecutor = new InsertInstructionExecutor(
                dbPath,
                tableInfoHolder,
                transactionLogger,
                locksTable,
                dataPageFactory
        );

        CreateTableInstructionExecutor createTableInstructionExecutor = new CreateTableInstructionExecutor(
                dbPath,
                tableInfoHolder
        );

        SelectInstructionExecutor selectInstructionExecutor = new SelectInstructionExecutor(
                dbPath,
                tableInfoHolder,
                locksTable
        );

        DeleteInstructionExecutor deleteInstructionExecutor = new DeleteInstructionExecutor(
                dbPath,
                tableInfoHolder,
                locksTable,
                transactionLogger
        );

        UpdateInstructionExecutor updateInstructionExecutor = new UpdateInstructionExecutor(
                dbPath,
                tableInfoHolder,
                locksTable,
                transactionLogger
        );

        InstructionsExecutionManager executionManager = new InstructionsExecutionManager(
                createTableInstructionExecutor,
                insertInstructionExecutor,
                selectInstructionExecutor,
                deleteInstructionExecutor,
                updateInstructionExecutor
        );

        var controller = new DBMSController(
                executionManager,
                locksTable,
                transactionLogger
        );

        return new App(controller, id -> {
            try {
                uncommittedTransactionsSolver.rollbackUncommittedTransaction(dbPath, transactionsLogsPath, id);
            } catch (DBMSException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
