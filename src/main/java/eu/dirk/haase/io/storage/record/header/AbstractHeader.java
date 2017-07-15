package eu.dirk.haase.io.storage.record.header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

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
        // Sets the channel's position to the Header's first byte.
        seek(channel, true);
        // Initialize the given buffer.
        source.clear();
        write(source);
        // Write the content of the buffer to the channel.
        source.flip();
        int bytesWritten = channel.write(source);
        if (bytesWritten != headerLength) {
            throw new IOException("Insufficient number of bytes written:" +
                    " Count of bytes currently written "
                    + bytesWritten
                    + ", but expected amount is "
                    + headerLength);
        }
    }

    /**
     * Initialize this Header from a sequence of bytes from this channel using the given buffer.
     *
     * @param channel
     * @param target
     * @throws IOException
     */
    public void read(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        long prevStartPointer = getStartPointer();
        // Sets the channel's position to the Header's first byte.
        seek(channel, false);
        // Prepare ByteBuffer
        target.clear();
        target.limit(headerLength);
        // Read the content of the entity, to which this channel is connected, into the buffer.
        int bytesRead = channel.read(target);
        if (bytesRead != headerLength) {
            throw new IOException("Insufficient number of bytes read:" +
                    " Count of bytes currently read "
                    + bytesRead
                    + ", but expected amount is "
                    + headerLength);
        }
        // Initialize this Header.
        target.flip();
        read(target);
        checkConsistency(prevStartPointer);
    }

    private void checkConsistency(long prevStartPointer) throws IOException {
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


    /**
     * Sets the channel's position to this Header's first byte.
     *
     * @param channel
     * @throws IOException
     */
    public void seek(SeekableByteChannel channel, boolean isWriting) throws IOException {
        if (isWriting) {
            growAsNeeded(channel);
        }
        channel.position(getStartPointer());
    }

    /**
     * Sets the channel's position to this Header's first byte.
     *
     * @param channel
     * @throws IOException
     */
    public void growAsNeeded(SeekableByteChannel channel) throws IOException {
        long minRequiredBytes = startPointer + headerLength;
        if (minRequiredBytes > channel.size()) {
            if (isGrowingAsNeeded) {
                channel.position(minRequiredBytes);
                ByteBuffer endMark = ByteBuffer.allocate(4);
                endMark.putInt(1);
                endMark.flip();
                channel.write(endMark);
            } else {
                throw new IOException("Unable to seek beyond the current size:" +
                        " Current size is "
                        + channel.size()
                        + ", but requested size was "
                        + minRequiredBytes
                        + " bytes.");
            }
        }
    }
}
