package db.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import db.DBMSException;
import db.fs.FileUtilsConcurrent;
import db.fs.SerializerBase;

/**
 * @author vlad333rrty
 */
public class TransactionLogEntrySerializer extends SerializerBase<List> {
    public static final TransactionLogEntrySerializer INSTANCE = new TransactionLogEntrySerializer();

    private TransactionLogEntrySerializer() {
        super(List.class);
    }

    public void serialize(Object object, Path path) throws DBMSException {
        try {
            List<TransactionLogEntry> oldEntries = Files.exists(path) ? deserialize(path) : List.of();
            List<TransactionLogEntry> newEntries  = (List<TransactionLogEntry>) object;
            List<TransactionLogEntry> finalEntries = Stream.concat(oldEntries.stream(), newEntries.stream()).toList();
            byte[] bytes = new ObjectMapper().writer().writeValueAsBytes(finalEntries);
            FileUtilsConcurrent.write(path, bytes);
        } catch (Exception e) {
            throw new DBMSException("Serialization error", e);
        }
    }

    @Override
    public List<TransactionLogEntry> deserialize(Path path) throws DBMSException {
        try {
            return new ObjectMapper().readerForListOf(TransactionLogEntry.class).readValue(path.toFile());
        } catch (IOException e) {
            throw new DBMSException("Deserialization error", e);
        }
    }
}
