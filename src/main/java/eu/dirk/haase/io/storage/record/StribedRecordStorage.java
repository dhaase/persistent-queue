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

    private final AtomicReferenceArray<RecordStorage> recordStorageArray;
    private final RecordStorage[] recordStorages;

    public StribedRecordStorage(int count, File file, OpenOption... options) throws IOException {
        this(count, file.toPath(), options);
    }

    public StribedRecordStorage(int count, Path path, OpenOption... options) throws IOException {
        this.stribeCount = count;
        this.count = count;
        this.shared = new Shared();
        RecordChannelStorage recordChannelStorage;
        recordStorages = new RecordStorage[count];
        for (int i = 0; count > i; ++i) {
            recordStorages[i] = recordChannelStorage = new RecordChannelStorage(path, options);
            recordChannelStorage.setShared(this.shared);
        }
        this.recordStorageArray = new AtomicReferenceArray<RecordStorage>(recordStorages);
    }

    private RecordStorage getRecordStorage() {
        long id = Thread.currentThread().getId();
        long index = (id % this.stribeCount);
        return recordStorages[(int) index]; //recordStorageArray.get((int) index);
    }

    @Override
    public void create() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            recordStorageArray.get(0).create();
            for (int i = 1; count > i; ++i) {
                recordStorageArray.get(i).initialize();
            }
        }
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            for (int i = 0; count > i; ++i) {
                recordStorageArray.get(i).initialize();
            }
        }
    }

    @Override
    public int selectRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        return getRecordStorage().selectRecord(key, dataBuffer);
    }

    @Override
    public int updateRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        return getRecordStorage().updateRecord(key, dataBuffer);
    }

    @Override
    public int deleteRecord(byte[] key) throws IOException, InterruptedException {
        return getRecordStorage().deleteRecord(key);
    }

    @Override
    public int insertRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException {
        return getRecordStorage().insertRecord(key, dataBuffer);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        synchronized (recordStorageArray) {
            for (int i = 0; count > i; ++i) {
                recordStorageArray.get(i).close();
            }
        }
    }
}
