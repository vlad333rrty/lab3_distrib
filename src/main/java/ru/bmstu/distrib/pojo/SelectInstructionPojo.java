package ru.bmstu.distrib.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import db.instructions.AttributesCondition;
import db.instructions.SelectInstruction;

/**
 * @author vlad333rrty
 */
public record SelectInstructionPojo(
        @JsonProperty("table_name") String tableName,
        @JsonProperty("selected_attributes") List<String> selectedAttributes,
        @JsonProperty("attribute_conditions") List<AttributeConditionPojo> attributeConditions
)
{
    public SelectInstruction toSelectInstruction() {
        List<AttributesCondition> attributesConditions = attributeConditions.stream()
                .map(x -> new AttributesCondition(x.attributeName(), x.expectedValue()))
                .toList();
        return new SelectInstruction(tableName, selectedAttributes, attributesConditions);
    }
}
