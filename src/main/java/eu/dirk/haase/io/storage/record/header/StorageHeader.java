package eu.dirk.haase.io.storage.record.header;

import java.nio.ByteBuffer;

/**
 * Created by dhaa on 15.07.17.
 */
final public class StorageHeader extends AbstractHeader {

    private final static int VERSION = 1;

    private final static int SUB_HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader (first Layout-Part see AbstractHeader.SUB_HEADER_LENGTH)
        headerLength += 4; // size of int for version
        headerLength += 4; // size of int for dataBlockCount
        headerLength += 4; // size of int for maxDataBlockLength
        headerLength += 4; // size of int for minDataBlockLength

        SUB_HEADER_LENGTH = headerLength;
    }

    /**
     * Structure Version of the storage unit.
     */
    private int version = VERSION;
    /**
     * Count of current blocks in the storage unit.
     */
    private int dataBlockCount;
    /**
     * Maximum length of the blocks in the storage unit.
     */
    private int maxDataBlockLength;
    /**
     * Minimum length of the blocks in the storage unit.
     */
    private int minDataBlockLength;
    public StorageHeader() {
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
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putInt(version);
        buffer.putInt(dataBlockCount);
        buffer.putInt(maxDataBlockLength);
        buffer.putInt(minDataBlockLength);
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        version = buffer.getInt();
        dataBlockCount = buffer.getInt();
        maxDataBlockLength = buffer.getInt();
        minDataBlockLength = buffer.getInt();
    }

    public int getDataBlockCount() {
        return dataBlockCount;
    }

    public void setDataBlockCount(int dataBlockCount) {
        this.dataBlockCount = dataBlockCount;
    }

    public int getMaxDataBlockLength() {
        return maxDataBlockLength;
    }

    public void setMaxDataBlockLength(int maxDataBlockLength) {
        this.maxDataBlockLength = maxDataBlockLength;
    }

    public int getMinDataBlockLength() {
        return minDataBlockLength;
    }

    public void setMinDataBlockLength(int minDataBlockLength) {
        this.minDataBlockLength = minDataBlockLength;
    }
}
