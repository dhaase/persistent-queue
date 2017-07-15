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

    private long minRequiredPosition;
    private int expectedSize;
    private long currentPosition;

    public void setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
    }

    public void setMinRequiredPosition(long minRequiredPosition) {
        this.minRequiredPosition = minRequiredPosition;
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
    public void writeAt(SeekableByteChannel channel, ByteBuffer source) throws IOException {
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
    public void readAt(SeekableByteChannel channel, ByteBuffer target) throws IOException {
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

    abstract protected void checkConsistency(long prevStartPointer) throws IOException;

    abstract protected void write(ByteBuffer buffer);

    abstract protected void read(ByteBuffer buffer);

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
        channel.position(currentPosition);
    }


    /**
     * Sets the channel's position to this Header's first byte.
     *
     * @param channel
     * @throws IOException
     */
    public void growAsNeeded(SeekableByteChannel channel) throws IOException {
        if (minRequiredPosition > channel.size()) {
            if (isGrowingAsNeeded) {
                channel.position(minRequiredPosition);
                ByteBuffer endMark = ByteBuffer.allocate(4);
                endMark.putInt(1);
                endMark.flip();
                channel.write(endMark);
            } else {
                throw new IOException("Unable to seek beyond the current size:" +
                        " Current size is "
                        + channel.size()
                        + ", but requested position was "
                        + minRequiredPosition
                        + " bytes.");
            }
        }
    }

}
