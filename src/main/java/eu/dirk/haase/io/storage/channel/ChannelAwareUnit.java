package eu.dirk.haase.io.storage.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by dhaa on 15.07.17.
 */
public abstract class ChannelAwareUnit {

    private boolean isConstistent;

    public boolean isConstistent() {
        return isConstistent;
    }

    /**
     * Writes the Header as a sequence of bytes to this channel using the given buffer.
     *
     * @param channel
     * @param source
     * @throws IOException
     */
    public int write(SeekableByteChannel channel, ByteBuffer source) throws IOException {
        checkConsistency();
        // Sets the channel's position to the Header's first byte.
        channel.position(getStartPointer());
        // Prepare ByteBuffer
        source.clear();
        // Initialize the given buffer.
        write(source);
        // Write the content of the buffer to the channel.
        source.flip();
        int bytesWritten = channel.write(source);
        isConstistent = (bytesWritten == getLength());
        if (!isConstistent) {
            throw new IOException("Insufficient number of bytes written:" +
                    " Count of bytes currently written "
                    + bytesWritten
                    + ", but expected amount is "
                    + getLength());
        }
        return bytesWritten;
    }


    /**
     * Initialize this Header from a sequence of bytes from this channel using the given buffer.
     *
     * @param channel
     * @param target
     * @throws IOException
     */
    public int read(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        long prevStartPointer = getStartPointer();
        // Sets the channel's position to the Header's first byte.
        channel.position(getStartPointer());
        // Prepare ByteBuffer
        target.clear();
        target.limit(getLength());
        // Read the content of the entity, to which this channel is connected, into the buffer.
        int bytesRead = channel.read(target);
        isConstistent = (bytesRead == getLength());
        if (!isConstistent) {
            throw new IOException("Insufficient number of bytes read:" +
                    " Count of bytes currently read "
                    + bytesRead
                    + ", but expected amount is "
                    + getLength());
        }
        // Initialize this Header.
        target.flip();
        read(target);
        isConstistent = (prevStartPointer == getStartPointer());
        return bytesRead;
    }

    abstract protected void checkConsistency() throws IOException;

    abstract public int getLength();

    abstract public long getStartPointer();

    abstract protected void write(ByteBuffer buffer);

    abstract protected void read(ByteBuffer buffer);


}
