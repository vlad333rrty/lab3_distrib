package db.instructions.executors;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import db.DBMSException;
import db.entries.TableContents;
import db.entries.TableInfoHolder;
import db.entries.TableMetaInfo;
import db.entries.TableRow;
import db.fs.DataPage;
import db.instructions.AttributesCondition;
import db.instructions.RowsFilter;
import db.instructions.SelectInstruction;
import db.transaction.TransactionLocksTable;

/**
 * @author vlad333rrty
 */
public class SelectInstructionExecutor extends InstructionExecutor<SelectInstruction> {
    private final TableInfoHolder tableInfoHolder;
    private final TransactionLocksTable transactionLocksTable;

    public SelectInstructionExecutor(Path dbPath, TableInfoHolder tableInfoHolder, TransactionLocksTable transactionLocksTable) {
        super(dbPath);
        this.tableInfoHolder = tableInfoHolder;
        this.transactionLocksTable = transactionLocksTable;
    }

    @Override
    public void execute(SelectInstruction instruction, String ownerTransactionId) throws DBMSException {
        String tableName = instruction.getTableName();
        TableMetaInfo metaInfo = tableInfoHolder.getMetaInfo(tableName);

        List<String> absentAttributes = getAttributesAbsentInSchema(metaInfo, instruction.getSelectedAttributes());
        if (!absentAttributes.isEmpty()) {
            throw new DBMSException(
                    "Wrong select statement, attributes are absent in schema: %s".formatted(absentAttributes));
        }

        Map<String, Integer> attributeToPosition = IntStream.range(0, metaInfo.attributes().size())
                .mapToObj(i -> Map.entry(metaInfo.attributes().get(i).name(), i))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<Integer, Object> attribute2ExpectedValue = instruction.getConditions().stream().collect(Collectors.toMap(
                x -> attributeToPosition.get(x.attributeName()),
                AttributesCondition::expectedValue
        ));

        TableContents tableContents = tableInfoHolder.getTableContents(tableName);

        List<List<Object>> filteredRows = gatherFilteredRows(
                tableContents.getPages(), attribute2ExpectedValue, ownerTransactionId);
        List<Integer> valueIndices = instruction.getSelectedAttributes().stream().map(attributeToPosition::get).toList();

        List<List<Object>> result = new ArrayList<>();
        for (List<Object> values : filteredRows) {
            result.add(valueIndices.stream().map(values::get).toList());
        }

        // todo we need to pass it further, not to print

        prettyPrintSelect(result);
    }


    private void prettyPrintSelect(List<List<Object>> result) {
        result.forEach(System.out::println);
    }

    private List<List<Object>> gatherFilteredRows(
            List<DataPage> pages,
            Map<Integer, Object> attributeIndex2ExpectedValue,
            String ownerTransactionId)
    {
        List<List<Object>> result = new ArrayList<>();
        for (DataPage page : pages) {
            transactionLocksTable.addLock(ownerTransactionId, page.getLock());
            List<TableRow> rows = page.getRecords();
            for (TableRow row : rows) {
                if (RowsFilter.doesRowFitCondition(row.values(), attributeIndex2ExpectedValue)) {
                    result.add(row.values());
                }
            }
        }

        return result;
    }

    private List<String> getAttributesAbsentInSchema(TableMetaInfo metaInfo, List<String> selectedAttributes) {
        Set<String> attributes = metaInfo.attributes().stream()
                .map(TableMetaInfo.Attribute::name)
                .collect(Collectors.toSet());
        return selectedAttributes.stream()
                .filter(attribute -> !attributes.contains(attribute))
                .collect(Collectors.toList());
    }
}
