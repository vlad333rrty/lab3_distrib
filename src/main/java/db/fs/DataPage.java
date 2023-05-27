package db.fs;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import db.entries.TableRow;

/**
 * @author vlad333rrty
 */
public class DataPage {
    public static final int CAPACITY = 10;

    @JsonProperty
    private final AtomicReference<List<TableRow>> records;

    @JsonIgnore
    private final PageLockWrapper lock;

    @JsonProperty
    private final int number;


    public DataPage(int number) {
        this.number = number;
        this.records = new AtomicReference<>(new ArrayList<>());
        this.lock = new PageLockWrapper(new ReentrantLock(), number);
    }

    @JsonCreator
    public DataPage(@JsonProperty("number") int number, @JsonProperty("records") List<TableRow> records) {
        this.number = number;
        this.records = new AtomicReference<>(records);
        this.lock = new PageLockWrapper(new ReentrantLock(), number);
    }

    public DataPage(int number, List<TableRow> records, PageLockWrapper lock) {
        this.number = number;
        this.records = new AtomicReference<>(records);
        this.lock = lock;
    }

    public void addRecord(TableRow row) {
        records.get().add(row);
    }

    public List<TableRow> getRecords() {
        return records.get();
    }

    public void setRecords(List<TableRow> records) {
        this.records.set(records);
    }

    public int number() {
        return number;
    }

    public PageLockWrapper getLock() {
        return lock;
    }

    public DataPage withRows(List<TableRow> rows) {
        return new DataPage(
                number,
                rows,
                lock
        );
    }

    public static class PageLockWrapper {
        private final Lock lock;
        private final int number;

        public PageLockWrapper(Lock lock, int number) {
            this.lock = lock;
            this.number = number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PageLockWrapper that)) return false;
            return number == that.number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(number);
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }
    }
}
