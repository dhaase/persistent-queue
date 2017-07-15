package eu.dirk.haase.io.storage.record.header;

import java.nio.ByteBuffer;

/**
 * Created by dhaa on 15.07.17.
 */
public abstract class AbstractHeader {

    private final static int HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Layout of the AbstractHeader
        headerLength += 8; // size of long for startPointer

        HEADER_LENGTH = headerLength;
    }

    /**
     * Overall length of the AbstractHeader.
     */
    private final int headerLength;
    /**
     * Start pointer to the first byte of the header within the storage unit.
     */
    private long startPointer;

    protected AbstractHeader(int subHeaderLength) {
        this.headerLength = HEADER_LENGTH + subHeaderLength;
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public long getStartPointer() {
        return startPointer;
    }

    protected void setStartPointer(long startPointer) {
        this.startPointer = startPointer;
    }

    protected void write(ByteBuffer buffer) {
        buffer.putLong(startPointer);
    }

    protected void read(ByteBuffer buffer) {
        startPointer = buffer.getLong();
    }
}
