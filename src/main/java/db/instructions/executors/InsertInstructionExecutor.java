package db.instructions.executors;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import db.DBMSException;
import db.PathUtils;
import db.backup.TransactionLogEntry;
import db.backup.TransactionLogManager;
import db.entries.TableContents;
import db.entries.TableInfoHolder;
import db.entries.TableMetaInfo;
import db.entries.TableRow;
import db.entries.TableRowFactory;
import db.fs.DataPage;
import db.fs.DataPageFSManager;
import db.fs.DataPageFactory;
import db.fs.TableSchemaSerializer;
import db.instructions.InsertInstruction;
import db.instructions.Instruction;
import db.instructions.ValuesTuple;
import db.transaction.TransactionLocksTable;

/**
 * @author vlad333rrty
 */
public class InsertInstructionExecutor extends InstructionExecutor<InsertInstruction> {

    private final TableInfoHolder tableInfoHolder;
    private final TransactionLogManager transactionLogger;
    private final TransactionLocksTable locksTable;
    private final DataPageFactory dataPageFactory;

    public InsertInstructionExecutor(
            Path dbPath,
            TableInfoHolder tableInfoHolder,
            TransactionLogManager transactionLogger,
            TransactionLocksTable locksTable,
            DataPageFactory dataPageFactory)
    {
        super(dbPath);
        this.tableInfoHolder = tableInfoHolder;
        this.transactionLogger = transactionLogger;
        this.locksTable = locksTable;
        this.dataPageFactory = dataPageFactory;
    }

    @Override
    public void execute(InsertInstruction instruction, String ownerTransactionId) throws DBMSException {
        String tableName = instruction.getTableName();

        TableMetaInfo tableMetaInfo = getTableSchema(tableName);
        List<TableRow> rows = getRowsToInsert(tableMetaInfo, instruction);

        TableContents table = tableInfoHolder.getTableContents(tableName);

        List<DataPageAndTableRow> dataPageAndTableRows = addRows(table, rows, ownerTransactionId);
        List<TransactionLogEntry> transactionLogEntries = getTransactionLogEntries(dataPageAndTableRows, ownerTransactionId, tableName);
        transactionLogger.log(transactionLogEntries, ownerTransactionId);
        dataPageAndTableRows.forEach(dataPageAndTableRow -> dataPageAndTableRow.dataPage.addRecord(dataPageAndTableRow.row));

        Path tablePath = PathUtils.getTablePath(getDbPath()).resolve(tableName);
        DataPageFSManager.dump(tablePath, dataPageAndTableRows.stream().map(x -> x.dataPage).toList());
    }

    private List<TransactionLogEntry> getTransactionLogEntries(
            List<DataPageAndTableRow> dataPageAndTableRows,
            String transactionId,
            String tableName)
    {
        return dataPageAndTableRows.stream().map(x -> new TransactionLogEntry(
                transactionId,
                Instruction.Type.INSERT.name(),
                String.valueOf(x.dataPage.number()),
                null,
                x.row,
                tableName
        )).toList();
    }

    private List<DataPageAndTableRow> addRows(TableContents table, List<TableRow> rows, String transactionId) {
        if (rows.isEmpty()) {
            return List.of();
        }
        List<DataPageAndTableRow> dataPageAndTableRows = new ArrayList<>();
        Iterator<TableRow> rowIterator = rows.iterator();
        for (int i = 0; i < table.getPages().size() && rowIterator.hasNext(); i++) {
            DataPage page = table.getPages().get(i);
            locksTable.addLock(transactionId, page.getLock());
            for (int j = page.getRecords().size(); j < DataPage.CAPACITY && rowIterator.hasNext(); j++) {
                dataPageAndTableRows.add(new DataPageAndTableRow(page, rowIterator.next()));
            }
        }

        while (rowIterator.hasNext()) {
            DataPage dataPage = dataPageFactory.createDataPage();
            table.getPages().add(dataPage);
            locksTable.addLock(transactionId, dataPage.getLock());
            for (int i = 0; i < DataPage.CAPACITY && rowIterator.hasNext(); i++) {
                dataPageAndTableRows.add(new DataPageAndTableRow(dataPage, rowIterator.next()));
            }
        }

        return dataPageAndTableRows;
    }

    private record DataPageAndTableRow(DataPage dataPage, TableRow row) {}

    private record DataPageWithPath(DataPage dataPage, Path tablePath) {}

    private TableMetaInfo getTableSchema(String tableName) throws DBMSException {
        TableMetaInfo tableMetaInfo = tableInfoHolder.getMetaInfo(tableName);
        if (tableMetaInfo == null) {
            Path tablePath = PathUtils.getTablePath(getDbPath()).resolve(tableName);
            if (!Files.exists(tablePath)) {
                throw new DBMSException("Failed to perform insert: %s table does not exists".formatted(tableName));
            }
            return TableSchemaSerializer.INSTANCE.deserialize(PathUtils.getSchemaPath(tablePath));
        }
        return tableMetaInfo;
    }


    private List<TableRow> getRowsToInsert(TableMetaInfo tableMetaInfo, InsertInstruction instruction) throws DBMSException {
        List<TableMetaInfo.Attribute> attributes = tableMetaInfo.attributes();
        List<ValuesTuple> valuesTuples = instruction.getValuesTuples();

        List<List<Object>> rows = getValues(attributes, valuesTuples);

        List<TableRow> resultRows = new ArrayList<>();
        for (var row : rows) {
            try {
                resultRows.add(TableRowFactory.createTableRow(row));
            } catch (NumberFormatException e) {
                throw new DBMSException("Incompatible type", e);
            }
        }

        return resultRows;
    }

    private List<List<Object>> getValues(List<TableMetaInfo.Attribute> attributes, List<ValuesTuple> valuesTuples) throws DBMSException {
        List<List<Object>> values = new ArrayList<>();
        for (ValuesTuple valuesTuple : valuesTuples) {
            List<Object> tuple = valuesTuple.valuesTuple();
            if (tuple.size() != attributes.size()) {
                throw new DBMSException("Wrong tuple size. Expected %s, actual %s".formatted(attributes.size(), tuple.size()));
            }
            List<Object> temp = new ArrayList<>();
            for (int i = 0; i < attributes.size(); i++) {
                temp.add(tuple.get(i));
            }
            values.add(temp);
        }
        return values;
    }
}
