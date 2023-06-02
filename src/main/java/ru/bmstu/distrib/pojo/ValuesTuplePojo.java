package ru.bmstu.distrib.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author vlad333rrty
 */
public record ValuesTuplePojo(
        @JsonProperty("values_tuple") List<Object> valuesTuple
)
{
}
