package db.instructions;

import java.util.List;

import db.DBMSException;


/**
 * @author vlad333rrty
 */
public class InsertInstruction extends Instruction {
    private final String tableName;
    private final List<ValuesTuple> valuesTuples;

    public InsertInstruction(String tableName, List<ValuesTuple> valuesTuples) {
        this.tableName = tableName;
        this.valuesTuples = valuesTuples;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ValuesTuple> getValuesTuples() {
        return valuesTuples;
    }

    @Override
    public void execute(InstructionsExecutor executor) throws DBMSException {
        executor.execute(this);
    }
}
