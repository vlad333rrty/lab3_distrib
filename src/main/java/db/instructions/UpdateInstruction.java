package db.instructions;

import java.util.List;

/**
 * @author vlad333rrty
 */
public class UpdateInstruction extends Instruction {
    private final String tableName;
    private final List<AttributesCondition> conditions;
    private final List<AttributeAndValueToSet> valuesToSet;

    public UpdateInstruction(String tableName, List<AttributesCondition> conditions, List<AttributeAndValueToSet> valuesToSet) {
        this.tableName = tableName;
        this.conditions = conditions;
        this.valuesToSet = valuesToSet;
    }

    public String getTableName() {
        return tableName;
    }

    public List<AttributesCondition> getConditions() {
        return conditions;
    }

    public List<AttributeAndValueToSet> getValuesToSet() {
        return valuesToSet;
    }

    @Override
    public void execute(InstructionsExecutor executor) throws Exception {
        executor.execute(this);
    }
}
