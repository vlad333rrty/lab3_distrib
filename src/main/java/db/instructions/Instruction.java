package db.instructions;


/**
 * @author vlad333rrty
 */
public abstract class Instruction {

    public abstract void execute(InstructionsExecutor executor) throws Exception;

    public enum Type { // todo types which we log in transactions, we do not consider select here. Should be moved away from here
        INSERT, UPDATE, DELETE, CREATE
    }
}
