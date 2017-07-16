package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.channel.ChannelAwareUnit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by dhaa on 15.07.17.
 */
public abstract class AbstractHeader extends ChannelAwareUnit {

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

    public int getLength() {
        return headerLength;
    }

    public long getEndPointer() {
        return getLength() + getStartPointer();
    }

    @Override
    public long getStartPointer() {
        return startPointer;
    }

    protected void setStartPointer(long startPointer) {
        this.startPointer = startPointer;
    }

    @Override
    protected void write(ByteBuffer buffer) {
        buffer.putLong(startPointer);
    }

    @Override
    protected void read(ByteBuffer buffer) {
        startPointer = buffer.getLong();
    }

    @Override
    protected void checkConsistency() throws IOException {
        if (startPointer < 0) {
            throw new IOException("startPointer can not be below 0: startPointer is currently "
                    + startPointer);

        }
    }
}
