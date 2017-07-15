package eu.dirk.haase.io.storage.record.header;

import java.nio.ByteBuffer;

/**
 * Created by dhaa on 15.07.17.
 */
final public class DataBlockHeader extends AbstractHeader {


    private final static int SUB_HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader (first Layout-Part see AbstractHeader.SUB_HEADER_LENGTH)
        headerLength += 8; // size of long for startDataPointer
        headerLength += 4; // size of int for dataBlockCapacity
        headerLength += 4; // size of int for dataBlockOccupied

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
    private int dataBlockOccupied;
    /**
     * Creates a fresh DataBlockHeader.
     */
    public DataBlockHeader() {
        super(SUB_HEADER_LENGTH);
    }

    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putLong(startDataPointer);
        buffer.putInt(dataBlockCapacity);
        buffer.putInt(dataBlockOccupied);
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        startDataPointer = buffer.getLong();
        dataBlockCapacity = buffer.getInt();
        dataBlockOccupied = buffer.getInt();
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

    public int getDataBlockOccupied() {
        return dataBlockOccupied;
    }

    public void setDataBlockOccupied(int dataBlockOccupied) {
        this.dataBlockOccupied = dataBlockOccupied;
    }

    @Override
    protected void setStartPointer(long startPointer) {
        super.setStartPointer(startPointer);
    }
}
