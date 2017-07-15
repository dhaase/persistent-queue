package eu.dirk.haase.io.file.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

import static eu.dirk.haase.io.file.record.BaseRecordsFile.*;


public class RecordHeader {

    /**
     * File pointer to the first byte of record data (8 bytes).
     */
    private long dataPointer;
    /**
     * Actual number of bytes of data held in this record (4 bytes).
     */
    private int dataCount;
    /**
     * Number of bytes of data that this record can hold (4 bytes).
     */
    private int dataCapacity;
    /**
     * Indicates this header's position in the file index.
     */
    private int indexPosition;

    private String key;

    public RecordHeader(int indexPosition) {
        this.indexPosition = indexPosition;
    }

    public RecordHeader(RecordHeader copyFromHeader) {
        this(copyFromHeader.key, copyFromHeader.indexPosition, copyFromHeader.dataPointer, copyFromHeader.dataCapacity, copyFromHeader.dataCount);
    }

    public RecordHeader(String key, int indexPosition, long dataPointer, int dataCapacity, int dataCount) {
        this.key = key;
        this.indexPosition = indexPosition;
        this.dataPointer = dataPointer;
        this.dataCapacity = dataCapacity;
        this.dataCount = dataCount;
    }

    public long getDataPointer() {
        return dataPointer;
    }

    public int getDataCount() {
        return dataCount;
    }

    public void setDataCount(int dataCount) {
        this.dataCount = dataCount;
    }

    public boolean isDataPointerWithinHeader(long targetDataPointer) {
        return (targetDataPointer >= dataPointer) && (targetDataPointer < (dataPointer + (long) dataCapacity));
    }

    public int getDataCapacity() {
        return dataCapacity;
    }

    public void setDataCapacity(int dataCapacity) {
        this.dataCapacity = dataCapacity;
    }

    public int increaseDataCapacity(int dataCapacityDelta) {
        return dataCapacity = (dataCapacity + dataCapacityDelta);
    }

    public int getIndexPosition() {
        return indexPosition;
    }

    public String getKey() {
        return key;
    }

    protected int getFreeSpace() {
        return dataCapacity - dataCount;
    }

    protected void read(DataInput in) throws IOException {
        dataPointer = in.readLong();
        dataCapacity = in.readInt();
        dataCount = in.readInt();
    }

    protected void write(DataOutput out) throws IOException {
        out.writeLong(dataPointer);
        out.writeInt(dataCapacity);
        out.writeInt(dataCount);
    }

    /**
     * Writes the ith record header to the index.
     */
    protected void writeRecordHeader(RandomAccessFile file) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(indexPosition));
        file.writeLong(dataPointer);
        file.writeInt(dataCapacity);
        file.writeInt(dataCount);
    }

    /**
     * Reads the ith record header from the index.
     */
    protected void readRecordHeader(RandomAccessFile file) {
        try {
            readKey(file);
            file.seek(indexPositionToRecordHeaderFp(indexPosition));
            read(file);
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }

    /**
     * Reads the ith key from the index.
     */
    protected String readKey(RandomAccessFile file) {
        try {
            file.seek(indexPositionToKeyFp(indexPosition));
            return key = file.readUTF();
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }

    /**
     * Reads the ith key from the index.
     */
    protected void writeKey(RandomAccessFile file) {
        try {
            file.seek(indexPositionToKeyFp(indexPosition));
            file.writeUTF(key);
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }

    /**
     * Returns a file pointer in the index pointing to the first byte
     * in the record pointer located at the given index position.
     */
    protected long indexPositionToRecordHeaderFp(int pos) {
        return indexPositionToKeyFp(pos) + MAX_KEY_LENGTH;
    }


    /**
     * Returns a file pointer in the index pointing to the first byte
     * in the key located at the given index position.
     */
    protected long indexPositionToKeyFp(int pos) {
        return FILE_HEADERS_REGION_LENGTH + (INDEX_ENTRY_LENGTH * pos);
    }

    /**
     * Returns a new record header which occupies the free space of this record.
     * Shrinks this record size by the size of its free space.
     */
    protected RecordHeader split(String key, int indexPosition) throws IOException {
        long newFp = dataPointer + (long) dataCount;
        RecordHeader newRecord = new RecordHeader(key, indexPosition, newFp, getFreeSpace(), 0);
        dataCapacity = dataCount;
        return newRecord;
    }


    protected RecordHeader relocateDataPointer(long dataPointer) throws IOException {
        RecordHeader relocatedHeader = new RecordHeader(this);
        relocatedHeader.dataPointer = dataPointer;
        return relocatedHeader;
    }


    protected RecordHeader relocate(int indexPosition) throws IOException {
        RecordHeader relocatedHeader = new RecordHeader(this);
        relocatedHeader.indexPosition = indexPosition;
        return relocatedHeader;
    }
}










