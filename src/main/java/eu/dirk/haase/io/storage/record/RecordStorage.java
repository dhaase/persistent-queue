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
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by dhaa on 15.07.17.
 */
public class RecordStorage {

    private final SeekableByteChannel channel;

    private final MainHeader mainHeader;

    private final ByteBuffer buffer;

    private final Path path;

    private RecordHeader lastRecordHeader;

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

    RecordHeader selectRecordHeader(byte[] key) throws IOException {
        RecordHeaderIteratorByKey iteratorByKey = new RecordHeaderIteratorByKey(null, key);
        if (iteratorByKey.hasNext()) {
            return iteratorByKey.next();
        }
        return null;
    }

    public int updateRecord(byte[] key, ByteBuffer data) throws IOException {
        RecordHeader recordHeader = selectRecordHeader(key);
        if (recordHeader != null) {
            recordHeader.setDeleted(true);
            recordHeader.write(this.channel, this.buffer);
            return insertRecord(key, data);
        }
        return -1;
    }

    public int deleteRecord(byte[] key) throws IOException {
        RecordHeader recordHeader = selectRecordHeader(key);
        if (recordHeader != null) {
            recordHeader.setDeleted(true);
            recordHeader.write(this.channel, this.buffer);
            return recordHeader.getRecordIndex();
        }
        return -1;
    }

    public int insertRecord(byte[] key, ByteBuffer data) throws IOException {
        RecordHeader recordHeader = findLastRecordHeader();
        if (recordHeader == null) {
            recordHeader = new RecordHeader();
        } else {
            recordHeader = recordHeader.nextHeader();
        }
        recordHeader.copyKey(key);
        recordHeader.initRecordDataLength(data);
        updateMainHeader(recordHeader);
        recordHeader.write(this.channel, this.buffer);
        return recordHeader.getRecordIndex();
    }

    protected void updateMainHeader(RecordHeader recordHeader) throws IOException {
        this.mainHeader.applyRecord(recordHeader);
        this.mainHeader.write(this.channel, this.buffer);
    }

    RecordHeader findLastRecordHeader() throws IOException {
        RecordHeaderIterator iterator = new RecordHeaderIteratorAlive(lastRecordHeader);
        RecordHeader nextRecordHeader = null;
        while (iterator.hasNext()) {
            nextRecordHeader = iterator.next();
        }
        return lastRecordHeader = nextRecordHeader;
    }

    public void close() throws IOException {
        this.mainHeader.write(this.channel, this.buffer);
        this.channel.close();
    }

    class RecordHeaderIterator implements Iterator<RecordHeader> {

        final long overallSize;

        RecordHeader nextRecordHeader = null;

        RecordHeaderIterator(RecordHeader startRecordHeader) throws IOException {
            overallSize = RecordStorage.this.channel.size();
            nextRecordHeader = (startRecordHeader != null ? startRecordHeader : new RecordHeader());
        }

        @Override
        public boolean hasNext() {
            try {
                if (nextRecordHeader.hasRoomForNext(overallSize)) {
                    nextRecordHeader.read(RecordStorage.this.channel, RecordStorage.this.buffer);
                    return nextRecordHeader.isValid();
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

    class RecordHeaderIteratorByKey extends RecordHeaderIterator {

        final byte[] key;

        RecordHeaderIteratorByKey(RecordHeader startRecordHeader, byte[] key) throws IOException {
            super(startRecordHeader);
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = super.hasNext();
            while (hasNext) {
                if (Arrays.equals(key, this.nextRecordHeader.getKey())) {
                    return true;
                } else {
                    next();
                    hasNext = super.hasNext();
                }
            }
            return false;
        }

    }


    class RecordHeaderIteratorAlive extends RecordHeaderIterator {

        RecordHeaderIteratorAlive(RecordHeader startRecordHeader) throws IOException {
            super(startRecordHeader);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = super.hasNext();
            while (hasNext) {
                if (!this.nextRecordHeader.isDeleted()) {
                    return true;
                } else {
                    next();
                    hasNext = super.hasNext();
                }
            }
            return false;
        }

    }

}
