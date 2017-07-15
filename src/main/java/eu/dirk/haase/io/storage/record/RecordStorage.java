package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.data.RecordData;
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
        this.path = path;
        this.channel = Files.newByteChannel(path, options);
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

    public int addRecord(ByteBuffer data, byte[] key) throws IOException {
        RecordHeader recordHeader = findLastRecordHeader();
        if (recordHeader == null) {
            recordHeader = new RecordHeader();
        }
        recordHeader = recordHeader.advanceRecordHeader();
        recordHeader.write(this.channel, this.buffer);
        return recordHeader.getRecordIndex();
    }

    RecordHeader findLastRecordHeader() throws IOException {
        long overallSize = this.channel.size();
        RecordHeader recordHeader = new RecordHeader();
        if(recordHeader.hasRoomForNext(overallSize)) {
            while (recordHeader.hasRoomForNext(overallSize)) {
                recordHeader = recordHeader.advanceRecordHeader();
            }
            return recordHeader;
        }
        return null;
    }

    public void close() throws IOException {
        channel.close();
    }
}
