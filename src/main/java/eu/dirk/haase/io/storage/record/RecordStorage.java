package eu.dirk.haase.io.storage.record;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by dhaa on 18.07.17.
 */
public interface RecordStorage {
    void create() throws IOException, InterruptedException;

    void initialize() throws IOException, InterruptedException;

    int selectRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException;

    int updateRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException;

    int deleteRecord(byte[] key) throws IOException, InterruptedException;

    int insertRecord(byte[] key, ByteBuffer dataBuffer) throws IOException, InterruptedException;

    void close() throws IOException, InterruptedException;
}
