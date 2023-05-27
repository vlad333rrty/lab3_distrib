package db.fs;

import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import db.DBMSException;

/**
 * @author vlad333rrty
 */
public abstract class SerializerBase<T> {
    private final Class<T> clazz;


    public SerializerBase(Class<T> clazz) {
        this.clazz = clazz;
    }

    public void serialize(Object object, Path path) throws DBMSException {
        try {
            byte[] bytes = new ObjectMapper().writer().writeValueAsBytes(object);
            FileUtilsConcurrent.write(path, bytes);
        } catch (Exception e) {
            throw new DBMSException("Serialization error", e);
        }
    }

    public T deserialize(Path path) throws DBMSException {
        try {
            return new ObjectMapper().readValue(path.toFile(), clazz);
        } catch (IOException e) {
            throw new DBMSException("Deserialization error", e);
        }
    }
}
