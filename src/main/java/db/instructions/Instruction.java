package db.instructions;


/**
 * @author vlad333rrty
 */
public abstract class Instruction {

    public abstract void execute(InstructionsExecutor executor) throws Exception;

    public enum Type {
        INSERT, UPDATE, DELETE, CREATE
    }
}
