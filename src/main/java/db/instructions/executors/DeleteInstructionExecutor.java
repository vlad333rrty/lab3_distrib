package db.instructions.executors;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import db.DBMSException;
import db.PathUtils;
import db.backup.TransactionLogEntry;
import db.backup.TransactionLogManager;
import db.entries.TableContents;
import db.entries.TableInfoHolder;
import db.entries.TableMetaInfo;
import db.entries.TableRow;
import db.fs.DataPage;
import db.fs.DataPageFSManager;
import db.instructions.AttributesCondition;
import db.instructions.DeleteInstruction;
import db.instructions.Instruction;
import db.instructions.RowsFilter;
import db.transaction.TransactionLocksTable;

/**
 * @author vlad333rrty
 */
public class DeleteInstructionExecutor extends InstructionExecutor<DeleteInstruction> {
    private final TableInfoHolder tableInfoHolder;
    private final TransactionLocksTable transactionLocksTable;
    private final TransactionLogManager transactionLogger;

    public DeleteInstructionExecutor(
            Path dbPath,
            TableInfoHolder tableInfoHolder,
            TransactionLocksTable transactionLocksTable,
            TransactionLogManager transactionLogger)
    {
        super(dbPath);
        this.tableInfoHolder = tableInfoHolder;
        this.transactionLocksTable = transactionLocksTable;
        this.transactionLogger = transactionLogger;
    }

    @Override
    public void execute(DeleteInstruction instruction, String ownerTransactionId) throws DBMSException {
        int qwe = 0/0;
        String tableName = instruction.getTableName();
        TableMetaInfo metaInfo = tableInfoHolder.getMetaInfo(tableName);

        Map<String, Integer> attributeToPosition = IntStream.range(0, metaInfo.attributes().size())
                .mapToObj(i -> Map.entry(metaInfo.attributes().get(i).name(), i))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<String> unknownAttributes =
                getUnknownConditionalAttributes(instruction.getConditions(), attributeToPosition.keySet());
        if (!unknownAttributes.isEmpty()) {
            throw new DBMSException("Unknown attributes %s".formatted(unknownAttributes));
        }

        Map<Integer, Object> attribute2ExpectedValue = instruction.getConditions().stream().collect(Collectors.toMap(
                x -> attributeToPosition.get(x.attributeName()),
                AttributesCondition::expectedValue
        ));

        TableContents tableContents = tableInfoHolder.getTableContents(tableName);

        Map<Integer, Set<Integer>> id2DeletedRows = getDataPageWithRowIndicesToDelete(
                tableContents.getPages(),
                attribute2ExpectedValue,
                ownerTransactionId
        );

        if (!id2DeletedRows.isEmpty()) {
            Map<Integer, DataPage> id2DataPage = tableContents.getPages().stream().collect(Collectors.toMap(
                    DataPage::number,
                    Function.identity()
            ));
            List<DataPageWithRowIndex> dataPageWithRowIndices = id2DeletedRows.entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream().map(x -> new DataPageWithRowIndex(id2DataPage.get(entry.getKey()), x)))
                    .toList();
            List<TransactionLogEntry> logEntries = getTransactionLogEntries(dataPageWithRowIndices, ownerTransactionId, tableName);
            transactionLogger.log(logEntries, ownerTransactionId);

            Map<Integer, DataPage> id2ModifiedPage = applyDeletion(id2DeletedRows, id2DataPage);

            updateTableContents(id2DataPage, id2ModifiedPage);

            DataPageFSManager.dump(PathUtils.getTablePath(getDbPath()).resolve(tableName), id2DataPage.values());
        }
    }

    private void updateTableContents(Map<Integer, DataPage> id2Page, Map<Integer, DataPage> id2ModifiedPage) {
        id2ModifiedPage.forEach((id, page) -> id2Page.get(id).setRecords(page.getRecords()));
    }

    private Map<Integer, DataPage> applyDeletion(
            Map<Integer, Set<Integer>> id2DeletedIndices,
            Map<Integer, DataPage> id2Page)
    {
        Map<Integer, DataPage> id2ModifiedPage = new HashMap<>();

        for (var entry : id2DeletedIndices.entrySet()) {
            DataPage page = id2Page.get(entry.getKey());
            List<TableRow> resultRows = new ArrayList<>();
            List<TableRow> records = page.getRecords();
            for (int i = 0; i < records.size(); i++) {
                if (!entry.getValue().contains(i)) {
                    resultRows.add(records.get(i));
                }
            }
            id2ModifiedPage.put(page.number(), page.withRows(resultRows));
        }

        return id2ModifiedPage;
    }

    private List<TransactionLogEntry> getTransactionLogEntries(
            List<DataPageWithRowIndex> modifiedPages,
            String transactionId,
            String tableName)
    {
        return modifiedPages.stream().map(x -> new TransactionLogEntry(
                transactionId,
                Instruction.Type.DELETE.name(),
                String.valueOf(x.page.number()),
                x.page.getRecords().get(x.rowIndex),
                null,
                tableName
        )).toList();
    }

    private List<String> getUnknownConditionalAttributes(
            List<AttributesCondition> conditionalAttributes,
            Set<String> knownAttributes)
    {
        return conditionalAttributes.stream()
                .map(AttributesCondition::attributeName)
                .filter(attribute -> !knownAttributes.contains(attribute))
                .toList();
    }

    public Map<Integer, Set<Integer>> getDataPageWithRowIndicesToDelete(
            List<DataPage> pages,
            Map<Integer, Object> attributeIndex2ExpectedValue,
            String ownerTransactionId)
    {
        Map<Integer, Set<Integer>> modifiedPages = new HashMap<>();
        for (DataPage page : pages) {
            transactionLocksTable.addLock(ownerTransactionId, page.getLock());
            List<TableRow> rows = page.getRecords();
            for (int i = 0; i < rows.size(); i++) {
                TableRow row = rows.get(i);
                if (RowsFilter.doesRowFitCondition(row.values(), attributeIndex2ExpectedValue)) {
                    modifiedPages.computeIfAbsent(page.number(), k -> new HashSet<>()).add(i);
                }
            }
        }
        return modifiedPages;
    }

    private record DataPageWithRowIndex(DataPage page, int rowIndex) {}
}
