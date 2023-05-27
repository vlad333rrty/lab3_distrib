package db.fs;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author vlad333rrty
 */
public class DataPageFactory {
    private final AtomicInteger count;

    public DataPageFactory(int maxPageNumber) {
        this.count = new AtomicInteger(maxPageNumber);
    }

    public DataPage createDataPage() {
        return new DataPage(count.incrementAndGet());
    }
}
