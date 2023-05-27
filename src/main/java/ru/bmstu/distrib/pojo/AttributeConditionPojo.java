package ru.bmstu.distrib.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author vlad333rrty
 */
public record AttributeConditionPojo(
        @JsonProperty("attribute_name") String attributeName,
        @JsonProperty("expected_value") Object expectedValue)
{
}
