package db;

import java.nio.file.Path;

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

    public App(DBMSController dbmsController) {
        this.dbmsController = dbmsController;
    }

    public static App getInstance(Path dbPath) throws DBMSException {
        DBMSController controller = prepareConfiguration(dbPath);
        return new App(controller);
    }

    public void run(Transaction transaction) throws Exception {
        dbmsController.handleTransaction(transaction);
    }

    private static DBMSController prepareConfiguration(Path dbPath) throws DBMSException {
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

        return new DBMSController(
                executionManager,
                locksTable,
                transactionLogger
        );
    }
}
