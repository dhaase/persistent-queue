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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dhaa on 15.07.17.
 */
public class RecordStorage {

    private final SeekableByteChannel channel;

    private final MainHeader mainHeader;

    private final ByteBuffer headerBuffer;

    private final Path path;

    private final RecordData currRecordData;

    private final RecordHeader currRecordHeader;

    private final Set<OpenOption> mutableOpenOptionSet = new HashSet<OpenOption>();
    private final Set<OpenOption> openOptionSet = Collections.unmodifiableSet(mutableOpenOptionSet);
    private final int overallRecordHeaderLength;
    private final AtomicLong tailPointer;
    private final AtomicInteger nextRecordIndex;
    private final Shared shared;
    private RecordHeader lastRecordHeader;

    public RecordStorage(File file, OpenOption... options) throws IOException {
        this(file.toPath(), options);
    }

    public RecordStorage(Path path, OpenOption... options) throws IOException {
        this(path, Files.newByteChannel(path, options));
        fillOpenOptionSet(options);
    }

    public RecordStorage(Path path, SeekableByteChannel channel) throws IOException {
        this.path = path;
        this.channel = channel;
        this.mainHeader = new MainHeader();
        this.currRecordData = new RecordData();
        this.currRecordHeader = new RecordHeader();
        this.overallRecordHeaderLength = this.currRecordData.getLength() + this.currRecordHeader.getLength();
        this.headerBuffer = ByteBuffer.allocate(calcBufferCapacity());
        this.tailPointer = new AtomicLong(this.mainHeader.getLength());
        this.nextRecordIndex = new AtomicInteger(0);
        this.shared = new Shared();
    }

    private int calcBufferCapacity() {
        int bufferCapacity = this.overallRecordHeaderLength + this.mainHeader.getLength();
        // round n up to nearest multiple of m
        int n = bufferCapacity;
        int m = 1024;
        return (n >= 0 ? ((n + m - 1) / m) * m : (n / m) * m);
    }

    private void fillOpenOptionSet(OpenOption[] options) {
        for (OpenOption oo : options) {
            this.mutableOpenOptionSet.add(oo);
        }
    }

    public Set<OpenOption> openOptions() {
        return openOptionSet;
    }

    public Path getPath() {
        return this.path;
    }

    public void create() throws IOException {
        this.mainHeader.write(this.channel, this.headerBuffer);
    }

    public void initialize() throws IOException {
        this.mainHeader.read(this.channel, this.headerBuffer);
    }

    public int selectRecord(byte[] key, ByteBuffer dataBuffer) throws IOException {
        RecordHeader recordHeader = selectRecordHeader(key);
        if ((recordHeader != null) && !recordHeader.isDeleted()) {
            currRecordData.initFromRecordHeader(recordHeader);
            currRecordData.read(this.channel, dataBuffer);
            currRecordData.readData(this.channel, dataBuffer);
            return recordHeader.getRecordIndex();
        }
        return -1;
    }

    public int updateRecord(byte[] key, ByteBuffer dataBuffer) throws IOException {
        RecordHeader recordHeader = deleteRecordHeader(key);
        if (recordHeader != null) {
            return insertRecord(key, dataBuffer);
        }
        return -1;
    }

    public int deleteRecord(byte[] key) throws IOException {
        RecordHeader recordHeader = deleteRecordHeader(key);
        if (recordHeader != null) {
            return recordHeader.getRecordIndex();
        }
        return -1;
    }

    public int insertRecord(byte[] key, ByteBuffer dataBuffer) throws IOException {

        int nextIndex = nextRecordIndex.getAndIncrement();

        int dataLength = shared.calcRecordLength(dataBuffer);
        long nextRecordStartPointer = shared.next(dataLength);

        this.currRecordHeader.init(nextRecordStartPointer, nextIndex, dataLength);
        this.currRecordHeader.copyKey(key);

        this.currRecordHeader.initRecordDataLength(dataBuffer);
        this.currRecordData.initFromRecordHeader(this.currRecordHeader);
        this.mainHeader.initFromRecordHeader(this.currRecordHeader);

        this.currRecordData.writeData(this.channel, dataBuffer);
        this.currRecordData.write(this.channel, this.headerBuffer);
        this.currRecordHeader.write(this.channel, this.headerBuffer);
        this.mainHeader.write(this.channel, this.headerBuffer);

        return currRecordHeader.getRecordIndex();
    }


    public void close() throws IOException {
        this.channel.close();
    }

    MainHeader getMainHeader() {
        return this.mainHeader;
    }

    RecordHeader selectRecordHeader(byte[] key) throws IOException {
        RecordHeaderIteratorByKey iteratorByKey = new RecordHeaderIteratorByKey(null, key);
        if (iteratorByKey.hasNext()) {
            return iteratorByKey.next();
        }
        return null;
    }

    RecordHeader deleteRecordHeader(byte[] key) throws IOException {
        RecordHeader recordHeader = selectRecordHeader(key);
        if (recordHeader != null) {
            if (!recordHeader.isDeleted()) {
                recordHeader.setDeleted(true);
                recordHeader.write(this.channel, this.headerBuffer);
            }
            return recordHeader;
        }
        return null;
    }

    RecordHeader findLastRecordHeader() throws IOException {
        RecordHeaderIterator iterator = new RecordHeaderIterator(lastRecordHeader);
        RecordHeader nextRecordHeader = null;
        while (iterator.hasNext()) {
            nextRecordHeader = iterator.next();
        }
        return lastRecordHeader = nextRecordHeader;
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
                    nextRecordHeader.read(RecordStorage.this.channel, RecordStorage.this.headerBuffer);
                    return nextRecordHeader.isValid();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return false;
        }

        @Override
        public RecordHeader next() {
            RecordHeader currRecHeader = nextRecordHeader;
            nextRecordHeader = nextRecordHeader.nextHeader();
            return currRecHeader;
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
