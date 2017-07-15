package eu.dirk.haase.io.storage.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by dhaa on 15.07.17.
 */
public abstract class ChannelAwareUnit {

    private final static ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final boolean isGrowingAsNeeded = true;

    private long minRequiredEndPosition;
    private int expectedSize;
    private long currentPosition;

    public void setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
    }

    public void setMinRequiredEndPosition(long minRequiredEndPosition) {
        this.minRequiredEndPosition = minRequiredEndPosition;
    }

    public void setCurrentPosition(long currentPosition) {
        this.currentPosition = currentPosition;
    }

    /**
     * Writes the Header as a sequence of bytes to this channel using the given buffer.
     *
     * @param channel
     * @param source
     * @throws IOException
     */
    protected void write(SeekableByteChannel channel, ByteBuffer source) throws IOException {
        checkConsistency();
        // Sets the channel's position to the Header's first byte.
        seek(channel, true);
        // Prepare ByteBuffer
        source.clear();
        source.order(NATIVE_ORDER);
        // Initialize the given buffer.
        write(source);
        // Write the content of the buffer to the channel.
        source.flip();
        int bytesWritten = channel.write(source);
        if (bytesWritten != expectedSize) {
            throw new IOException("Insufficient number of bytes written:" +
                    " Count of bytes currently written "
                    + bytesWritten
                    + ", but expected amount is "
                    + expectedSize);
        }
    }


    /**
     * Initialize this Header from a sequence of bytes from this channel using the given buffer.
     *
     * @param channel
     * @param target
     * @throws IOException
     */
    protected void read(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        long prevStartPointer = currentPosition;
        // Sets the channel's position to the Header's first byte.
        seek(channel, false);
        // Prepare ByteBuffer
        target.clear();
        target.limit(expectedSize);
        target.order(NATIVE_ORDER);
        // Read the content of the entity, to which this channel is connected, into the buffer.
        int bytesRead = channel.read(target);
        if (bytesRead != expectedSize) {
            throw new IOException("Insufficient number of bytes read:" +
                    " Count of bytes currently read "
                    + bytesRead
                    + ", but expected amount is "
                    + expectedSize);
        }
        // Initialize this Header.
        target.flip();
        read(target);
        checkConsistency(prevStartPointer);
    }

    abstract protected void checkConsistency() throws IOException;

    protected void checkConsistency(long prevStartPointer) throws IOException {
        checkConsistency();
        if (prevStartPointer != getStartPointer()) {
            throw new IOException("Unexpected change of StartPointer:" +
                    " StartPointer was before "
                    + prevStartPointer
                    + ", but is now "
                    + getStartPointer());
        }
    }

    abstract public long getStartPointer();

    abstract protected void write(ByteBuffer buffer);

    abstract protected void read(ByteBuffer buffer);

    /**
     * Sets the channel's position to this Header's first byte.
     *
     * @param channel
     * @throws IOException
     */
    protected void seek(SeekableByteChannel channel, boolean isWriting) throws IOException {
        if (isWriting) {
            growAsNeeded(channel);
        }
        channel.position(currentPosition);
    }


    /**
     * Sets the channel's position to this Header's first byte.
     *
     * @param channel
     * @throws IOException
     */
    private void growAsNeeded(SeekableByteChannel channel) throws IOException {
        if (minRequiredEndPosition > channel.size()) {
            if (isGrowingAsNeeded) {
                channel.position(minRequiredEndPosition);
                ByteBuffer endMark = ByteBuffer.allocate(4);
                endMark.putInt(1);
                endMark.flip();
                channel.write(endMark);
            } else {
                throw new IOException("Unable to seek beyond the current size:" +
                        " Current size is "
                        + channel.size()
                        + ", but requested position was "
                        + minRequiredEndPosition
                        + " bytes.");
            }
        }
    }

}
