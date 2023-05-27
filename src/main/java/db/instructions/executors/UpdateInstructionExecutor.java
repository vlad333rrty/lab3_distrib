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
import db.instructions.AttributeAndValueToSet;
import db.instructions.AttributesCondition;
import db.instructions.Instruction;
import db.instructions.RowsFilter;
import db.instructions.UpdateInstruction;
import db.transaction.TransactionLocksTable;

/**
 * @author vlad333rrty
 */
public class UpdateInstructionExecutor extends InstructionExecutor<UpdateInstruction> {
    private final TableInfoHolder tableInfoHolder;
    private final TransactionLocksTable transactionLocksTable;
    private final TransactionLogManager transactionLogger;

    public UpdateInstructionExecutor(
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
    public void execute(UpdateInstruction instruction, String ownerTransactionId) throws DBMSException {
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

        Map<Integer, Set<Integer>> id2UpdatedRows = getDataPageWithRowIndicesToUpdate(
                tableContents.getPages(),
                attribute2ExpectedValue,
                ownerTransactionId
        );


        if (!id2UpdatedRows.isEmpty()) {
            Map<Integer, DataPage> id2DataPage = tableContents.getPages().stream().collect(Collectors.toMap(
                    DataPage::number,
                    Function.identity()
            ));
            Map<Integer, DataPage> id2ModifiedPages = updateRowValues(
                    attributeToPosition,
                    id2UpdatedRows,
                    id2DataPage,
                    instruction.getValuesToSet());
            logTransaction(id2DataPage, id2ModifiedPages, id2UpdatedRows, ownerTransactionId, tableName);

            updateTableContents(id2DataPage, id2ModifiedPages);

            DataPageFSManager.dump(PathUtils.getTablePath(getDbPath()).resolve(tableName), id2DataPage.values());
        }
    }

    private void updateTableContents(Map<Integer, DataPage> id2Page, Map<Integer, DataPage> id2ModifiedPage) {
        id2ModifiedPage.forEach((id, page) -> id2Page.get(id).setRecords(page.getRecords()));
    }


    private void logTransaction(
            Map<Integer, DataPage> id2Page,
            Map<Integer, DataPage> id2ModifiedPage,
            Map<Integer, Set<Integer>> id2UpdatedRows,
            String transactionId,
            String tableName) throws DBMSException
    {
        List<TableRowUpdateInfoWithDataPageNumber> updateInfoWithDataPageNumberList =
                getTableRowUpdateInfoWithDataPageNumberList(id2Page, id2ModifiedPage, id2UpdatedRows);
        List<TransactionLogEntry> logEntries = updateInfoWithDataPageNumberList.stream().map(x -> new TransactionLogEntry(
                transactionId,
                Instruction.Type.UPDATE.name(),
                String.valueOf(x.pageId),
                x.before,
                x.after,
                tableName
        )).toList();
        transactionLogger.log(logEntries, transactionId);
    }

    private List<TableRowUpdateInfoWithDataPageNumber> getTableRowUpdateInfoWithDataPageNumberList(
            Map<Integer, DataPage> id2Page,
            Map<Integer, DataPage> id2ModifiedPage,
            Map<Integer, Set<Integer>> id2UpdatedRows)
    {
        return id2ModifiedPage.entrySet().stream().flatMap(entry ->
                        id2UpdatedRows.get(entry.getKey()).stream().map(i ->
                                new TableRowUpdateInfoWithDataPageNumber(
                                        id2Page.get(entry.getKey()).getRecords().get(i),
                                        id2ModifiedPage.get(entry.getKey()).getRecords().get(i),
                                        entry.getValue().number())))
                .toList();
    }


    private Map<Integer, DataPage> updateRowValues(
            Map<String, Integer> attributeToPosition,
            Map<Integer, Set<Integer>> id2RowsToUpdate,
            Map<Integer, DataPage> id2DataPage,
            List<AttributeAndValueToSet> valuesToSet)
    {
        Map<Integer, DataPage> id2UpdatedPage = new HashMap<>();
        for (var entry : id2RowsToUpdate.entrySet()) {
            DataPage page = id2DataPage.get(entry.getKey());
            List<TableRow> updatedRows = new ArrayList<>(page.getRecords().size());
            List<TableRow> records = page.getRecords();
            for (int i = 0; i < records.size(); i++) {
                TableRow row = page.getRecords().get(i).withValues(new ArrayList<>(page.getRecords().get(i).values()));
                if (entry.getValue().contains(i)) {
                    for (AttributeAndValueToSet attributeAndValueToSet : valuesToSet) {
                        int attributePos = attributeToPosition.get(attributeAndValueToSet.attributeName());
                        row.values().set(attributePos, attributeAndValueToSet.value());
                    }
                }
                updatedRows.add(row);
            }
            id2UpdatedPage.put(page.number(), page.withRows(updatedRows));
        }
        return id2UpdatedPage;
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

    public Map<Integer, Set<Integer>> getDataPageWithRowIndicesToUpdate(
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


    private record TableRowUpdateInfoWithDataPageNumber(
            TableRow before,
            TableRow after,
            int pageId) {
    }
}
