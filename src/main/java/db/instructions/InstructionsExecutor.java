package db.instructions;

import db.DBMSException;

/**
 * @author vlad333rrty
 */
public interface InstructionsExecutor {
    void execute(CreateTableInstruction createTableInstruction) throws Exception;

    void execute(InsertInstruction insertInstruction) throws DBMSException;

    void execute(SelectInstruction selectInstruction) throws DBMSException;

    void execute(DeleteInstruction deleteInstruction) throws DBMSException;

    void execute(UpdateInstruction updateInstruction) throws DBMSException;
}
