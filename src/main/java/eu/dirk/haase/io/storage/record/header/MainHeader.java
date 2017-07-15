package eu.dirk.haase.io.storage.record.header;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by dhaa on 15.07.17.
 */
final public class MainHeader extends AbstractHeader {

    /**
     * Prolog is a text which describes the origin or purpose of the storages unit (= the file).
     */
    private final static byte[] PROLOG = MainHeader.class.getCanonicalName().getBytes();

    private final static int VERSION = 1;

    private final static int SUB_HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader
        // (first Layout-Part see AbstractHeader.HEADER_LENGTH)
        headerLength += 4; // size of int for version
        headerLength += 4; // size of int for recordCount
        headerLength += 4; // size of int for maxRecordDataLength
        headerLength += 4; // size of int for minRecordDataLength
        headerLength += PROLOG.length; // size of byte[] for PROLOG

        SUB_HEADER_LENGTH = headerLength;
    }

    /**
     * Structure Version of the storage unit.
     */
    private int version = VERSION;
    /**
     * Count of current blocks in the storage unit.
     */
    private int recordCount;
    /**
     * Maximum length of the blocks in the storage unit.
     */
    private int maxRecordDataLength;
    /**
     * Minimum length of the blocks in the storage unit.
     */
    private int minRecordDataLength;


    public MainHeader() {
        super(SUB_HEADER_LENGTH);
        setStartPointer(0);
    }

    public int getVersion() {
        return version;
    }

    public boolean isCompabible() {
        return (version == VERSION);
    }

    @Override
    protected void checkConsistency() throws IOException {
        super.checkConsistency();
        if (maxRecordDataLength < minRecordDataLength) {
            throw new IOException("maxRecordDataLength can not be less than minRecordDataLength:" +
                    " maxRecordDataLength is currently "
                    + maxRecordDataLength
                    + " and minRecordDataLength is "
                    + minRecordDataLength);
        }
        if (recordCount < 0) {
            throw new IOException("recordCount can not be below 0:" +
                    " recordCount is currently "
                    + recordCount);

        }
        if (!isCompabible()) {
            throw new IOException("Version is incompatible:" +
                    " Version is currently "
                    + version
                    + " and expected Version is "
                    + VERSION);

        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putInt(version);
        buffer.putInt(recordCount);
        buffer.putInt(maxRecordDataLength);
        buffer.putInt(minRecordDataLength);
        buffer.put(PROLOG);
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        version = buffer.getInt();
        recordCount = buffer.getInt();
        maxRecordDataLength = buffer.getInt();
        minRecordDataLength = buffer.getInt();
        // Skip prolog - never read
        buffer.position(buffer.position() + PROLOG.length);
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void calcRecordLengthMinMax(int recordDataLength) {
        maxRecordDataLength = Math.max(recordDataLength, maxRecordDataLength);
        minRecordDataLength = Math.min(recordDataLength, minRecordDataLength);
    }

    public int incrementRecordCount() {
        return ++recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public int getMaxRecordDataLength() {
        return maxRecordDataLength;
    }

    public void setMaxRecordDataLength(int maxRecordDataLength) {
        this.maxRecordDataLength = maxRecordDataLength;
    }

    public int getMinRecordDataLength() {
        return minRecordDataLength;
    }

    public void setMinRecordDataLength(int minRecordDataLength) {
        this.minRecordDataLength = minRecordDataLength;
    }
}
