package eu.dirk.haase.io.storage.record.header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dhaa on 15.07.17.
 */
final public class RecordHeader extends AbstractHeader {

    private final static String TIMESTAMP_STR = "2017-01-01 00:00:00";

    private final static long MIN_TIMESTAMP = Timestamp.valueOf(TIMESTAMP_STR).getTime();

    private final static int SUB_HEADER_LENGTH;

    private final static MainHeader MAIN_HEADER = new MainHeader();

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader
        // (first Layout-Part see AbstractHeader.HEADER_LENGTH)
        headerLength += 8; // size of long for startDataPointer
        headerLength += 4; // size of int for recordDataCapacity
        headerLength += 4; // size of int for recordDataLength
        headerLength += 4; // size of int for recordIndex
        headerLength += 8; // size of int for lastModifiedTimeMillis

        SUB_HEADER_LENGTH = headerLength;
    }

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
     * Creates a fresh RecordHeader.
     */
    public RecordHeader() {
        super(SUB_HEADER_LENGTH);
        initFirstHeader();
    }


    private void initFirstHeader() {
        lastModifiedTimeMillis = System.currentTimeMillis();
        setStartPointer(MAIN_HEADER.getEndPointer());
        setStartDataPointer(getEndPointer());
        setRecordDataCapacity(0);
        setRecordDataLength(0);
        setRecordIndex(0);
    }


    public RecordHeader advanceRecordHeader() {
        RecordHeader nextRecordHeader = new RecordHeader();

        nextRecordHeader.setStartPointer(getEndPointer());
        nextRecordHeader.setStartDataPointer(getEndPointer() + getLength());
        nextRecordHeader.setRecordDataCapacity(0);
        nextRecordHeader.setRecordDataLength(0);
        nextRecordHeader.setRecordIndex(getRecordIndex() + 1);

        return nextRecordHeader;
    }


    public boolean hasRoomForNext(long overallSize) {
        return (getEndPointer() < overallSize);
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
        buffer.putLong(System.currentTimeMillis());
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        startDataPointer = buffer.getLong();
        recordDataCapacity = buffer.getInt();
        recordDataLength = buffer.getInt();
        recordIndex = buffer.getInt();
        lastModifiedTimeMillis = buffer.getLong();
    }

    public long getStartDataPointer() {
        return startDataPointer;
    }

    public void setStartDataPointer(long startDataPointer) {
        this.startDataPointer = startDataPointer;
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

}
