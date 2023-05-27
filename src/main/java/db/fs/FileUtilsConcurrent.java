package db.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author vlad333rrty
 */
public class FileUtilsConcurrent {
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static void write(Path path, byte[] bytes) throws IOException {
        lock.writeLock().lock();
        Files.write(path, bytes);
        lock.writeLock().unlock();
    }

    public static byte[] read(Path path) throws IOException {
        lock.readLock().lock();
        byte[] result = Files.readAllBytes(path);
        lock.readLock().unlock();
        return result;
    }
}
