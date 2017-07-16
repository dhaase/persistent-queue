package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.header.MainHeader;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 * Created by dhaa on 15.07.17.
 */
public class RecordStorage {

    private final SeekableByteChannel channel;

    private final MainHeader mainHeader;

    private final ByteBuffer buffer;

    private final Path path;

    public RecordStorage(File file, OpenOption... options) throws IOException {
        this(file.toPath(), options);
    }

    public RecordStorage(Path path, OpenOption... options) throws IOException {
        this(path, Files.newByteChannel(path, options));
    }

    public RecordStorage(Path path, SeekableByteChannel channel) throws IOException {
        this.path = path;
        this.channel = channel;
        this.mainHeader = new MainHeader();
        this.buffer = ByteBuffer.allocate(1024 * 10);
    }

    public MainHeader getMainHeader() {
        return this.mainHeader;
    }

    public Path getPath() {
        return this.path;
    }

    public void create() throws IOException {
        this.mainHeader.write(this.channel, this.buffer);
    }

    public void initialize() throws IOException {
        this.mainHeader.read(this.channel, this.buffer);
    }

    public int insertRecord(ByteBuffer data, byte[] key) throws IOException {
        RecordHeader recordHeader = findLastRecordHeader();
        if (recordHeader == null) {
            recordHeader = new RecordHeader();
        } else {
            recordHeader = recordHeader.advanceRecordHeader();
        }
        int dataLength = (data != null ? data.limit() : 0);
        recordHeader.setRecordDataCapacity(dataLength);
        recordHeader.setRecordDataLength(dataLength);
        this.mainHeader.applyRecord(recordHeader);
        this.mainHeader.write(this.channel, this.buffer);
        recordHeader.write(this.channel, this.buffer);
        return recordHeader.getRecordIndex();
    }

    RecordHeader findLastRecordHeader() throws IOException {
        long overallSize = this.channel.size();
        RecordHeader lastRecordHeader = null;
        RecordHeader currRecordHeader = new RecordHeader();
        while (currRecordHeader.hasRoomForNext(overallSize)) {
            currRecordHeader.read(this.channel, this.buffer);
            if (currRecordHeader.isConstistent()) {
                lastRecordHeader = currRecordHeader;
            }
            currRecordHeader = currRecordHeader.advanceRecordHeader();
        }
        return lastRecordHeader;
    }

    public void close() throws IOException {
        this.mainHeader.write(this.channel, this.buffer);
        this.channel.close();
    }
}
