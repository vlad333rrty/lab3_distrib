package db;

import java.io.IOException;

import db.backup.TransactionLogManager;
import db.instructions.Instruction;
import db.instructions.InstructionsExecutionManager;
import db.transaction.Transaction;
import db.transaction.TransactionLocksTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author vlad333rrty
 */
public class DBMSController {
    private static final Logger logger = LogManager.getLogger(DBMSController.class);

    private final InstructionsExecutionManager instructionsExecutionManager;
    private final TransactionLocksTable locksTable;
    private final TransactionLogManager transactionLogger;

    public DBMSController(
            InstructionsExecutionManager instructionsExecutionManager,
            TransactionLocksTable locksTable,
            TransactionLogManager transactionLogger)
    {
        this.instructionsExecutionManager = instructionsExecutionManager;
        this.locksTable = locksTable;
        this.transactionLogger = transactionLogger;
    }

    public void handleTransaction(Transaction transaction) throws Exception {
        for (Instruction instruction : transaction.instructions()) {
            instructionsExecutionManager.executeInstruction(instruction, transaction.id());
        }
        locksTable.unlockAll(transaction.id());
    }

    public void commitTransaction(String transactionId) throws IOException {
        transactionLogger.commit(transactionId);
    }
}
