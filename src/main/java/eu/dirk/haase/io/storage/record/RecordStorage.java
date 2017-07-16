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
import java.util.Iterator;

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
            recordHeader = recordHeader.nextHeader();
        }
        int dataLength = (data != null ? data.limit() : 0);
        recordHeader.setRecordDataCapacity(dataLength);
        recordHeader.setRecordDataLength(dataLength);
        updateMainHeader(recordHeader);
        recordHeader.write(this.channel, this.buffer);
        return recordHeader.getRecordIndex();
    }

    protected void updateMainHeader(RecordHeader recordHeader) throws IOException {
        this.mainHeader.applyRecord(recordHeader);
        this.mainHeader.write(this.channel, this.buffer);
    }

    RecordHeader findLastRecordHeader() throws IOException {
        RecordHeaderIterator iterator = new RecordHeaderIterator();
        RecordHeader nextRecordHeader = null;
        while (iterator.hasNext()) {
            nextRecordHeader = iterator.next();
        }
        return nextRecordHeader;
    }

    public void close() throws IOException {
        this.mainHeader.write(this.channel, this.buffer);
        this.channel.close();
    }

    class RecordHeaderIterator implements Iterator<RecordHeader> {

        final long overallSize;

        RecordHeader nextRecordHeader = null;

        RecordHeaderIterator() throws IOException {
            overallSize = RecordStorage.this.channel.size();
            nextRecordHeader = new RecordHeader();
        }

        @Override
        public boolean hasNext() {
            try {
                if (nextRecordHeader.hasRoomForNext(overallSize)) {
                    nextRecordHeader.read(RecordStorage.this.channel, RecordStorage.this.buffer);
                    return nextRecordHeader.isConstistent();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return false;
        }

        @Override
        public RecordHeader next() {
            RecordHeader currRecordHeader = nextRecordHeader;
            nextRecordHeader = nextRecordHeader.nextHeader();
            return currRecordHeader;
        }

        @Override
        public void remove() {
        }
    }
}
