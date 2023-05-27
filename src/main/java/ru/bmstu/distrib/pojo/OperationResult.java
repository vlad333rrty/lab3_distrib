package ru.bmstu.distrib.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author vlad333rrty
 */
public enum OperationResult {
    @JsonProperty("ok") OK,
    @JsonProperty("bad") BAD
}
