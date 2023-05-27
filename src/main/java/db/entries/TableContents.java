package db.entries;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import db.fs.DataPage;

/**
 * @author vlad333rrty
 */
public class TableContents {
    private final AtomicReference<List<DataPage>> pages;

    public TableContents(List<DataPage> pages) {
        this.pages = new AtomicReference<>(pages);
    }

    public List<DataPage> getPages() {
        return pages.get();
    }

    public void set(List<DataPage> pages) {
        this.pages.set(pages);
    }
}
