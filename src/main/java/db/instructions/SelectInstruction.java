package db.instructions;

import java.util.List;

/**
 * @author vlad333rrty
 */
public class SelectInstruction extends Instruction {
    private final String tableName;
    private final List<String> selectedAttributes;
    private final List<AttributesCondition> conditions;

    public SelectInstruction(
            String tableName,
            List<String> selectedAttributes,
            List<AttributesCondition> conditions)
    {
        this.tableName = tableName;
        this.selectedAttributes = selectedAttributes;
        this.conditions = conditions;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getSelectedAttributes() {
        return selectedAttributes;
    }

    public List<AttributesCondition> getConditions() {
        return conditions;
    }

    @Override
    public void execute(InstructionsExecutor executor) throws Exception {
        executor.execute(this);
    }
}
