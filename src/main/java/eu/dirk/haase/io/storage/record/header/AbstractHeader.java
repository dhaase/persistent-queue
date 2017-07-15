package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.channel.ChannelAwareUnit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

    private final boolean isGrowingAsNeeded = true;

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

    /**
     * Writes the Header as a sequence of bytes to this channel using the given buffer.
     *
     * @param channel
     * @param source
     * @throws IOException
     */
    public void write(SeekableByteChannel channel, ByteBuffer source) throws IOException {
        setCurrentPosition(startPointer);
        setMinRequiredPosition(startPointer + headerLength);
        setExpectedSize(headerLength);
        writeAt(channel, source);
    }

    /**
     * Initialize this Header from a sequence of bytes from this channel using the given buffer.
     *
     * @param channel
     * @param target
     * @throws IOException
     */
    public void read(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        setCurrentPosition(startPointer);
        setMinRequiredPosition(startPointer + headerLength);
        setExpectedSize(headerLength);
        readAt(channel, target);
    }

    protected void checkConsistency(long prevStartPointer) throws IOException {
        checkConsistency();
        if (prevStartPointer != startPointer) {
            throw new IOException("Unexpected change of StartPointer:" +
                    " StartPointer was before "
                    + prevStartPointer
                    + ", but is now "
                    + getStartPointer());
        }
    }


    protected void checkConsistency() throws IOException {
        if (startPointer < 0) {
            throw new IOException("startPointer can not be below 0: startPointer is currently "
                    + startPointer);

        }
    }
}
