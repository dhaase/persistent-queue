package eu.dirk.haase.io.file.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class RecordHeader {

    /**
     * File pointer to the first byte of record data (8 bytes).
     */
    protected long dataPointer;

    /**
     * Actual number of bytes of data held in this record (4 bytes).
     */
    protected int dataCount;

    /**
     * Number of bytes of data that this record can hold (4 bytes).
     */
    protected int dataCapacity;

    /**
     * Indicates this header's position in the file index.
     */
    protected int indexPosition;


    protected final String key;

    public RecordHeader(String key, int indexPosition) {
        this.key = key;
        this.indexPosition = indexPosition;
    }

    public RecordHeader(String key, long dataPointer, int dataCapacity) {
        if (dataCapacity < 1) {
            throw new IllegalArgumentException("Bad record size: " + dataCapacity);
        }
        this.key = key;
        this.dataPointer = dataPointer;
        this.dataCapacity = dataCapacity;
        this.dataCount = 0;
    }

    public String getKey() {
        return key;
    }

    protected void setIndexPosition(int indexPosition) {
        this.indexPosition = indexPosition;
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
     * Returns a new record header which occupies the free space of this record.
     * Shrinks this record size by the size of its free space.
     */
    protected RecordHeader split(String key) throws IOException {
        long newFp = dataPointer + (long) dataCount;
        RecordHeader newRecord = new RecordHeader(key, newFp, getFreeSpace());
        dataCapacity = dataCount;
        return newRecord;
    }

}










