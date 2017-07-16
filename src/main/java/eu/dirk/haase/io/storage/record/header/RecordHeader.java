package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.record.StorageUnit;
import eu.dirk.haase.io.storage.record.data.RecordData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Created by dhaa on 15.07.17.
 */
final public class RecordHeader extends AbstractHeader {

    private final static String TIMESTAMP_STR = "2017-01-01 00:00:00";

    private final static long MIN_TIMESTAMP = Timestamp.valueOf(TIMESTAMP_STR).getTime();

    private final static int KEY_LENGTH = UUID.randomUUID().toString().getBytes().length;

    private final static int SUB_HEADER_LENGTH;

    private final static MainHeader MAIN_HEADER = new MainHeader();

    private final static long MAGIC_DATA = StorageUnit.buildMagicData("RecHdr");

    private final static RecordData recordData = new RecordData();

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader
        // (first Layout-Part see AbstractHeader.HEADER_LENGTH)
        headerLength += 8; // size of long for startDataPointer
        headerLength += 4; // size of int for recordDataCapacity
        headerLength += 4; // size of int for recordDataLength
        headerLength += 4; // size of int for recordIndex
        headerLength += 4; // size of int for bitfield
        headerLength += 8; // size of int for lastModifiedTimeMillis
        headerLength += KEY_LENGTH; // size of the key

        SUB_HEADER_LENGTH = headerLength;
    }

    /**
     * Key of this Record.
     */
    private final byte[] key;
    /**
     * Start pointer to the first byte of the data within the storage unit.
     */
    private long startDataPointer;
    /**
     * Overall byte capacity of the current block.
     */
    private int recordDataCapacity;
    /**
     * Overall occupied bytes of the current block.
     */
    private int recordDataLength;
    /**
     * The last modified timestamp in milliseconds.
     * <p>
     * Milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    private long lastModifiedTimeMillis;
    /**
     * Index position of the Record.
     */
    private int recordIndex;
    /**
     * bitfield of the Record.
     */
    private int bitfield;

    /**
     * Creates a fresh RecordHeader.
     */
    public RecordHeader() {
        super(SUB_HEADER_LENGTH);
        this.key = new byte[KEY_LENGTH];
        initFirstHeader();
    }

    public byte[] getKey() {
        return key;
    }

    public void copyKey(byte[] keyData) {
        if ((keyData != null) && (keyData.length == key.length)) {
            System.arraycopy(keyData, 0, key, 0, key.length);
        } else {
            Arrays.fill(key, (byte) 0);
        }
    }


    @Override
    public long getMagicData() {
        return MAGIC_DATA;
    }

    private void initFirstHeader() {
        lastModifiedTimeMillis = System.currentTimeMillis();
        bitfield = 0;
        setStartPointer(MAIN_HEADER.getEndPointer());
        setStartDataPointer(getEndPointer());
        setRecordDataCapacity(0);
        setRecordDataLength(0);
        setRecordIndex(0);
    }


    public RecordHeader nextHeader() {
        RecordHeader nextRecordHeader = new RecordHeader();

        nextRecordHeader.lastModifiedTimeMillis = System.currentTimeMillis();
        nextRecordHeader.bitfield = 0;

        long nextStartPointer = getEndPointer() + recordData.getLength();

        nextRecordHeader.setStartPointer(nextStartPointer);
        nextRecordHeader.setStartDataPointer(nextStartPointer + getLength());
        nextRecordHeader.setRecordDataCapacity(0);
        nextRecordHeader.setRecordDataLength(0);
        nextRecordHeader.setRecordIndex(getRecordIndex() + 1);

        return nextRecordHeader;
    }


    public boolean hasRoomForNext(long overallSize) {
        return (getStartPointer() < overallSize);
    }

    public int getRecordIndex() {
        return recordIndex;
    }

    public void setRecordIndex(int recordIndex) {
        this.recordIndex = recordIndex;
    }

    public long getLastModifiedTimeMillis() {
        return lastModifiedTimeMillis;
    }

    @Override
    public List<String> enlistConsistencyErrors() throws IOException {
        List<String> errorReasonList = super.enlistConsistencyErrors();
        long storageHeaderEnd = (MAIN_HEADER.getStartPointer() + MAIN_HEADER.getLength());
        if (startDataPointer < storageHeaderEnd) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("startDataPointer can not be"
                    + " within the MainHeader"
                    + ": RecordHeader.startDataPointer is currently at "
                    + startDataPointer
                    + " and MainHeader ends at "
                    + storageHeaderEnd);
        }
        if (recordDataCapacity < 0) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("recordDataCapacity can not be below 0:" +
                    " recordDataCapacity is currently "
                    + recordDataCapacity);

        }
        if (recordDataLength < 0) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("recordDataLength can not be below 0:" +
                    " recordDataLength is currently "
                    + recordDataLength);

        }
        if (lastModifiedTimeMillis < MIN_TIMESTAMP) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("lastModifiedTimeMillis must be later than" +
                    " '" + TIMESTAMP_STR + "':" +
                    " lastModifiedTimeMillis is currently "
                    + lastModifiedTimeMillis
                    + "; Calculates to the Timestamp: "
                    + new Timestamp(lastModifiedTimeMillis));
        }
        return errorReasonList;
    }


    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putLong(startDataPointer);
        buffer.putInt(recordDataCapacity);
        buffer.putInt(recordDataLength);
        buffer.putInt(recordIndex);
        buffer.putInt(bitfield);
        buffer.putLong(System.currentTimeMillis());
        buffer.put(key);
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        startDataPointer = buffer.getLong();
        recordDataCapacity = buffer.getInt();
        recordDataLength = buffer.getInt();
        recordIndex = buffer.getInt();
        bitfield = buffer.getInt();
        lastModifiedTimeMillis = buffer.getLong();
        buffer.get(key);
    }

    public long getStartDataPointer() {
        return startDataPointer;
    }

    public void setStartDataPointer(long startDataPointer) {
        this.startDataPointer = startDataPointer;
    }

    public void initRecordDataLength(ByteBuffer data) {
        int dataLength = (data != null ? data.limit() : 0);
        recordDataCapacity = recordDataLength = dataLength;
    }

    public int getRecordDataCapacity() {
        return recordDataCapacity;
    }

    public void setRecordDataCapacity(int recordDataCapacity) {
        this.recordDataCapacity = recordDataCapacity;
    }

    public int getRecordDataLength() {
        return recordDataLength;
    }

    public void setRecordDataLength(int recordDataLength) {
        this.recordDataLength = recordDataLength;
    }

    @Override
    public void setStartPointer(long startPointer) {
        super.setStartPointer(startPointer);
    }

    public boolean isDeleted() {
        return Bitfield.testBit(Bit.DELETE, bitfield);
    }

    public void setDeleted(boolean isDeleted) {
        if (isDeleted) {
            bitfield = Bitfield.setBit(Bit.DELETE, bitfield);
        } else {
            bitfield = Bitfield.clearBit(Bit.DELETE, bitfield);
        }
    }

    enum Bit {
        DELETE
    }

    static class Bitfield {

        static boolean testBit(Bit bit, int bitfield) {
            return (bitfield & (1 << bit.ordinal())) == (1 << bit.ordinal());
        }

        static int setBit(Bit bit, int bitfield) {
            return (bitfield | (1 << bit.ordinal()));
        }

        static int clearBit(Bit bit, int bitfield) {
            return (bitfield ^ (1 << bit.ordinal()));
        }

    }
}
