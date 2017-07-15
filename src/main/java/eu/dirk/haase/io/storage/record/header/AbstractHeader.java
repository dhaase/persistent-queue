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
        seek(channel);
        // Initialize the given buffer.
        source.clear();
        write(source);
        // Write the content of the buffer to the channel.
        source.flip();
        channel.write(source);
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
        seek(channel);
        // Read the content of entity, to which this channel is connected, into the buffer.
        target.clear();
        channel.read(target);
        // Initialize this Header.
        target.flip();
        read(target);
        // Validate the content of entity
        if (prevStartPointer != getStartPointer()) {
            throw new IOException("Unexpected change of StartPointer: StartPointer was before "
                    + prevStartPointer
                    + ", but is now "
                    + getStartPointer());
        }
    }

    /**
     * Sets the channel's position to this Header's first byte.
     * @param channel
     * @throws IOException
     */
    public void seek(SeekableByteChannel channel) throws IOException {
        channel.position(getStartPointer());
    }
}
