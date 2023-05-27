package db.transaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import db.fs.DataPage;

/**
 * @author vlad333rrty
 */
public class TransactionLocksTable {
    private final Map<String, Set<DataPage.PageLockWrapper>> id2Locks = new ConcurrentHashMap<>();

    public void addLock(String id, DataPage.PageLockWrapper lock) {
       if (id2Locks.computeIfAbsent(id, key -> ConcurrentHashMap.newKeySet()).add(lock)) {
           lock.lock();
       }
    }

    public void unlockAll(String id) {
        id2Locks.computeIfPresent(
                id,
                (key, locks) -> {
                    locks.forEach(DataPage.PageLockWrapper::unlock);
                    return new HashSet<>();
                }
        );
    }
}
