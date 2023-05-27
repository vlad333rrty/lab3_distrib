package db.instructions;


import db.entries.TableMetaInfo;

/**
 * @author vlad333rrty
 */
public class CreateTableInstruction extends Instruction {

    private final TableMetaInfo metaInfo;

    public CreateTableInstruction(TableMetaInfo metaInfo) {
        this.metaInfo = metaInfo;
    }

    public TableMetaInfo getMetaInfo() {
        return metaInfo;
    }

    @Override
    public void execute(InstructionsExecutor executor) throws Exception {
        executor.execute(this);
    }
}
