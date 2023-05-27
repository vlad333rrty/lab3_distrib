package db.backup;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import db.entries.TableRow;

/**
 * @author vlad333rrty
 */
public class TransactionLogEntry {
    @JsonProperty
    private final String transactionId;
    @JsonProperty
    private final String instructionType;
    @JsonProperty
    private final String pageId;
    @JsonProperty
    private final TableRow beforeValue;
    @JsonProperty
    private final TableRow afterValue;
    @JsonProperty
    private final String tableName;

    @JsonCreator
    public TransactionLogEntry(
            @JsonProperty("transactionId")
            String transactionId,
            @JsonProperty("instructionType")
            String instructionType,
            @JsonProperty("pageId")
            String pageId,
            @JsonProperty("beforeValue")
            TableRow beforeValue,
            @JsonProperty("afterValue")
            TableRow afterValue,
            @JsonProperty("tableName")
            String tableName) {
        this.transactionId = transactionId;
        this.instructionType = instructionType;
        this.pageId = pageId;
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "TransactionLogEntry{" +
                "transactionId='" + transactionId + '\'' +
                ", instructionType='" + instructionType + '\'' +
                ", pageId='" + pageId + '\'' +
                ", beforeValue=" + beforeValue +
                ", afterValue=" + afterValue +
                '}';
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getInstructionType() {
        return instructionType;
    }

    public String getPageId() {
        return pageId;
    }

    public TableRow getBeforeValue() {
        return beforeValue;
    }

    public TableRow getAfterValue() {
        return afterValue;
    }

    public String getTableName() {
        return tableName;
    }
}
