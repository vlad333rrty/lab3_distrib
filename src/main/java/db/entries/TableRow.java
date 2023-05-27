package db.entries;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author vlad333rrty
 */
public record TableRow(@JsonProperty String fqdn, @JsonProperty List<Object> values) implements Serializable {
    public TableRow withValues(List<Object> values) {
        return new TableRow(fqdn, values);
    }
}
