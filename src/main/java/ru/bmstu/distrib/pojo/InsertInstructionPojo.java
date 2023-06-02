package ru.bmstu.distrib.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import db.instructions.InsertInstruction;
import db.instructions.ValuesTuple;

/**
 * @author vlad333rrty
 */
public record InsertInstructionPojo(
        @JsonProperty("table_name") String tableName,
        @JsonProperty("values_tuple") List<ValuesTuplePojo> valuesTuplePojo
)
{
    public InsertInstruction toInsertInstruction() {
        return new InsertInstruction(tableName, valuesTuplePojo.stream().map(x -> new ValuesTuple(x.valuesTuple())).toList());
    }
}
