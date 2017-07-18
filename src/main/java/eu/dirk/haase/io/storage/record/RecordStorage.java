package eu.dirk.haase.io.storage.record;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by dhaa on 18.07.17.
 */
public interface RecordStorage {
    void create() throws IOException;

    void initialize() throws IOException;

    int selectRecord(byte[] key, ByteBuffer dataBuffer) throws IOException;

    int updateRecord(byte[] key, ByteBuffer dataBuffer) throws IOException;

    int deleteRecord(byte[] key) throws IOException;

    int insertRecord(byte[] key, ByteBuffer dataBuffer) throws IOException;

    void close() throws IOException;
}
