package eu.dirk.haase.io.storage.record.header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

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
        headerLength += 4; // size of int for dataBlockCapacity
        headerLength += 4; // size of int for occupiedBytes
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
    private int dataBlockCapacity;
    /**
     * Overall occupied bytes of the current block.
     */
    private int occupiedBytes;
    /**
     * The last modified timestamp in milliseconds.
     * <p>
     * Milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    private long lastModifiedTimeMillis;

    /**
     * Creates a fresh RecordHeader.
     */
    public RecordHeader() {
        super(SUB_HEADER_LENGTH);
        lastModifiedTimeMillis = System.currentTimeMillis();
    }

    public long getLastModifiedTimeMillis() {
        return lastModifiedTimeMillis;
    }

    @Override
    protected void checkConsistency() throws IOException {
        super.checkConsistency();
        long storageHeaderEnd = (MAIN_HEADER.getStartPointer() + MAIN_HEADER.getHeaderLength());
        if (startDataPointer < storageHeaderEnd) {
            throw new IOException("startDataPointer can not be"
                    + " within the MainHeader"
                    + ": RecordHeader.startDataPointer is currently at "
                    + startDataPointer
                    + " and MainHeader ends at "
                    + storageHeaderEnd);
        }
        if (dataBlockCapacity < 0) {
            throw new IOException("dataBlockCapacity can not be below 0:" +
                    " dataBlockCapacity is currently "
                    + dataBlockCapacity);

        }
        if (occupiedBytes < 0) {
            throw new IOException("occupiedBytes can not be below 0:" +
                    " occupiedBytes is currently "
                    + occupiedBytes);

        }
        if (lastModifiedTimeMillis < MIN_TIMESTAMP) {
            throw new IOException("lastModifiedTimeMillis must be later than" +
                    " '" + TIMESTAMP_STR +"':" +
                    " lastModifiedTimeMillis is currently "
                    + lastModifiedTimeMillis
                    + "; Calculates to the Timestamp: "
                    + new Timestamp(lastModifiedTimeMillis));
        }
    }


    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putLong(startDataPointer);
        buffer.putInt(dataBlockCapacity);
        buffer.putInt(occupiedBytes);
        buffer.putLong(System.currentTimeMillis());
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        startDataPointer = buffer.getLong();
        dataBlockCapacity = buffer.getInt();
        occupiedBytes = buffer.getInt();
        lastModifiedTimeMillis = buffer.getLong();
    }

    public long getStartDataPointer() {
        return startDataPointer;
    }

    public void setStartDataPointer(long startDataPointer) {
        this.startDataPointer = startDataPointer;
    }

    public int getDataBlockCapacity() {
        return dataBlockCapacity;
    }

    public void setDataBlockCapacity(int dataBlockCapacity) {
        this.dataBlockCapacity = dataBlockCapacity;
    }

    public int getOccupiedBytes() {
        return occupiedBytes;
    }

    public void setOccupiedBytes(int occupiedBytes) {
        this.occupiedBytes = occupiedBytes;
    }

    @Override
    protected void setStartPointer(long startPointer) {
        super.setStartPointer(startPointer);
    }
}
