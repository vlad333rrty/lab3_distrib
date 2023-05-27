package db.instructions.executors;

import java.nio.file.Path;

import db.instructions.Instruction;

/**
 * @author vlad333rrty
 */
public abstract class InstructionExecutor<T extends Instruction> {
    private final Path dbPath;

    public InstructionExecutor(Path dbPath) {
        this.dbPath = dbPath;
    }

    protected Path getDbPath() {
        return dbPath;
    }

    public abstract void execute(T instruction, String ownerTransactionId) throws Exception;
}
