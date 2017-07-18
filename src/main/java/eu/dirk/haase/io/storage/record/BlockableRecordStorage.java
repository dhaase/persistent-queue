package eu.dirk.haase.io.storage.record;

import java.util.concurrent.locks.Lock;

/**
 * Created by dhaa on 18.07.17.
 */
public interface BlockableRecordStorage extends RecordStorage {

    Lock readLock();

    Lock writeLock();

}
