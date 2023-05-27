package db.instructions;

import db.DBMSException;
import db.instructions.executors.CreateTableInstructionExecutor;
import db.instructions.executors.DeleteInstructionExecutor;
import db.instructions.executors.InsertInstructionExecutor;
import db.instructions.executors.SelectInstructionExecutor;
import db.instructions.executors.UpdateInstructionExecutor;

/**
 * @author vlad333rrty
 */
public class InstructionsExecutionManager {
    private final CreateTableInstructionExecutor createTableInstructionExecutor;
    private final InsertInstructionExecutor insertInstructionExecutor;
    private final SelectInstructionExecutor selectInstructionExecutor;
    private final DeleteInstructionExecutor deleteInstructionExecutor;
    private final UpdateInstructionExecutor updateInstructionExecutor;

    public InstructionsExecutionManager(
            CreateTableInstructionExecutor createTableInstructionExecutor,
            InsertInstructionExecutor insertInstructionExecutor,
            SelectInstructionExecutor selectInstructionExecutor,
            DeleteInstructionExecutor deleteInstructionExecutor,
            UpdateInstructionExecutor updateInstructionExecutor)
    {
        this.createTableInstructionExecutor = createTableInstructionExecutor;
        this.insertInstructionExecutor = insertInstructionExecutor;
        this.selectInstructionExecutor = selectInstructionExecutor;
        this.deleteInstructionExecutor = deleteInstructionExecutor;
        this.updateInstructionExecutor = updateInstructionExecutor;
    }

    public void executeInstruction(Instruction instruction, String ownerTransactionId) throws Exception {
        instruction.execute(new InstructionsExecutor() {
            @Override
            public void execute(CreateTableInstruction createTableInstruction) throws Exception {
                createTableInstructionExecutor.execute(createTableInstruction, ownerTransactionId);
            }

            @Override
            public void execute(InsertInstruction insertInstruction) throws DBMSException {
                insertInstructionExecutor.execute(insertInstruction, ownerTransactionId);
            }

            @Override
            public void execute(SelectInstruction selectInstruction) throws DBMSException {
                selectInstructionExecutor.execute(selectInstruction, ownerTransactionId);
            }

            @Override
            public void execute(DeleteInstruction deleteInstruction) throws DBMSException {
                deleteInstructionExecutor.execute(deleteInstruction, ownerTransactionId);
            }

            @Override
            public void execute(UpdateInstruction updateInstruction) throws DBMSException {
                updateInstructionExecutor.execute(updateInstruction, ownerTransactionId);
            }
        });
    }
}
