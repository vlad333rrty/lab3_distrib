package ru.bmstu.distrib.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author vlad333rrty
 */
public enum OperationResult {
    OK, BAD;

    public static OperationResult fromString(String value) {
        return switch (value) {
            case "bad" -> BAD;
            case "ok" -> OK;
            default -> throw new IllegalArgumentException();
        };
    }
}
