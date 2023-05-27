package db.entries;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import db.ql.Type;

/**
 * @author vlad333rrty
 */
public record TableMetaInfo(@JsonProperty String tableName, @JsonProperty List<Attribute> attributes) {
    public record Attribute(@JsonProperty String name, @JsonProperty Type type) {
    }
}
