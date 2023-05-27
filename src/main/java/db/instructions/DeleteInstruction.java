package db.instructions;

import java.util.List;

/**
 * @author vlad333rrty
 */
public class DeleteInstruction extends Instruction {
    private final String tableName;
    private final List<AttributesCondition> conditions;

    public DeleteInstruction(String tableName, List<AttributesCondition> conditions) {
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public String getTableName() {
        return tableName;
    }

    public List<AttributesCondition> getConditions() {
        return conditions;
    }

    @Override
    public void execute(InstructionsExecutor executor) throws Exception {
        executor.execute(this);
    }
}
