package db.instructions.executors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import db.DBMSException;
import db.PathUtils;
import db.entries.TableInfoHolder;
import db.fs.TableSchemaSerializer;
import db.instructions.CreateTableInstruction;
import db.instructions.TableCreationException;

/**
 * @author vlad333rrty
 */
public class CreateTableInstructionExecutor extends InstructionExecutor<CreateTableInstruction> {

    private final TableInfoHolder tableInfoHolder;

    public CreateTableInstructionExecutor(Path dbPath, TableInfoHolder tableInfoHolder) {
        super(dbPath);
        this.tableInfoHolder = tableInfoHolder;
    }

    @Override
    public void execute(CreateTableInstruction createTableInstruction, String ownerTransactionId) throws TableCreationException {
        String tableName = createTableInstruction.getMetaInfo().tableName();
        Path tablePath = PathUtils.getTablePath(getDbPath()).resolve(createTableInstruction.getMetaInfo().tableName());
        if (Files.exists(tablePath)) {
            throw new TableCreationException("Failed to create table %s. Table already exists".formatted(tableName));
        }
        try {
            Files.createDirectories(tablePath);
            TableSchemaSerializer.INSTANCE.serialize(createTableInstruction.getMetaInfo(), tablePath);
            tableInfoHolder.addMetaInfo(tableName, createTableInstruction.getMetaInfo());
        } catch (DBMSException | IOException e) {
            throw new TableCreationException("Failed to create table %s".formatted(tableName), e);
        }
    }
}
