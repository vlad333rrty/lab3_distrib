package ru.bmstu.distrib.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import db.instructions.AttributesCondition;
import db.instructions.DeleteInstruction;

/**
 * @author vlad333rrty
 */
public record DeleteInstructionPojo(
        @JsonProperty("table_name") String tableName,
        @JsonProperty("attribute_conditions") List<AttributeConditionPojo> attributeConditions
)
{
    public DeleteInstruction toDeleteInstruction() {
        List<AttributesCondition> attributesConditions = this.attributeConditions.stream()
                .map(cond -> new AttributesCondition(cond.attributeName(), cond.expectedValue()))
                .toList();
        return new DeleteInstruction(tableName, attributesConditions);
    }
}
