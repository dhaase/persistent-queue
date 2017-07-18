package eu.dirk.haase.io.storage.record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by dhaa on 18.07.17.
 */
public class StribedRecordStorage implements RecordStorage {

    private final long stribeCount;
    private final int count;
    private final Shared shared;

    private final AtomicReferenceArray<BlockableRecordStorage> recordStorageArray;

    public StribedRecordStorage(int count, File file, OpenOption... options) throws IOException {
        this(count, file.toPath(), options);
    }

    public StribedRecordStorage(int count, Path path, OpenOption... options) throws IOException {
        this.stribeCount = count;
        this.count = count;
        this.shared = new Shared();
        RecordChannelStorage recordChannelStorage;
        BlockableRecordStorage[] recordStorages = new BlockableRecordStorage[count];
        for (int i = 0; count > i; ++i) {
            recordStorages[i] = recordChannelStorage = new RecordChannelStorage(path, options);
            recordChannelStorage.setShared(this.shared);
        }
        this.recordStorageArray = new AtomicReferenceArray<BlockableRecordStorage>(recordStorages);
    }

    private BlockableRecordStorage getRecordStorage() {
        long id = Thread.currentThread().getId();
        long index = (id % this.stribeCount);
        return recordStorageArray.get((int) index);
    }

    @Override
    public void create() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            BlockableRecordStorage recordStorage = recordStorageArray.get(0);
            recordStorage.writeLock().lockInterruptibly();
            try {
                recordStorage.create();
            } finally {
                recordStorage.writeLock().unlock();
            }
            for (int i = 1; count > i; ++i) {
                recordStorage = recordStorageArray.get(i);
                recordStorage.writeLock().lockInterruptibly();
                try {
                    recordStorage.initialize();
                } finally {
                    recordStorage.writeLock().unlock();
                }
            }
        }
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            for (int i = 0; count > i; ++i) {
                BlockableRecordStorage recordStorage = recordStorageArray.get(i);
                recordStorage.writeLock().lockInterruptibly();
                try {
                    recordStorage.initialize();
                } finally {
                    recordStorage.writeLock().unlock();
                }
            }
        }
    }

    @Override
    public int selectRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        BlockableRecordStorage recordStorage = getRecordStorage();
        recordStorage.readLock().lockInterruptibly();
        try {
            return recordStorage.selectRecord(key, dataBuffer);
        } finally {
            recordStorage.readLock().unlock();
        }
    }

    @Override
    public int updateRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        BlockableRecordStorage recordStorage = getRecordStorage();
        recordStorage.writeLock().lockInterruptibly();
        try {
            return getRecordStorage().updateRecord(key, dataBuffer);
        } finally {
            recordStorage.writeLock().unlock();
        }
    }

    @Override
    public int deleteRecord(byte[] key) throws IOException, InterruptedException {
        BlockableRecordStorage recordStorage = getRecordStorage();
        recordStorage.writeLock().lockInterruptibly();
        try {
            return getRecordStorage().deleteRecord(key);
        } finally {
            recordStorage.writeLock().unlock();
        }
    }

    @Override
    public int insertRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        BlockableRecordStorage recordStorage = getRecordStorage();
        recordStorage.writeLock().lockInterruptibly();
        try {
            return getRecordStorage().insertRecord(key, dataBuffer);
        } finally {
            recordStorage.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            for (int i = 0; count > i; ++i) {
                BlockableRecordStorage recordStorage = recordStorageArray.get(i);
                recordStorage.writeLock().lockInterruptibly();
                try {
                    recordStorage.close();
                } finally {
                    recordStorage.writeLock().unlock();
                }
            }
        }
    }
}
