package eu.dirk.haase.io.storage.record.data;

import eu.dirk.haase.io.storage.channel.ChannelAwareUnit;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by dhaa on 15.07.17.
 */
public class RecordData extends ChannelAwareUnit {

    private final RecordHeader recordHeader;

    private final ByteBuffer data;

    public RecordData(ByteBuffer data, byte[] key) {
        this.data = data;
        this.recordHeader = new RecordHeader();
        this.recordHeader.setRecordDataCapacity(data.position());
        this.recordHeader.setRecordDataLength(data.position());
    }

    public ByteBuffer getData() {
        return data;
    }

    public RecordHeader getRecordHeader() {
        return recordHeader;
    }

    /**
     * Writes the Header as a sequence of bytes to this channel using the given buffer.
     *
     * @param channel
     * @param source
     * @throws IOException
     */
    public void write(SeekableByteChannel channel, ByteBuffer source) throws IOException {
        initializeForIO();
        super.write(channel, source);
    }

    private void initializeForIO() {
        long startPointer = recordHeader.getStartDataPointer();
        int recordDataLength = recordHeader.getRecordDataLength();
        setCurrentPosition(startPointer);
        setMinRequiredEndPosition(startPointer + recordDataLength);
        setExpectedSize(recordDataLength);
    }

    /**
     * Initialize this Header from a sequence of bytes from this channel using the given buffer.
     *
     * @param channel
     * @param target
     * @throws IOException
     */
    public void read(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        initializeForIO();
        super.read(channel, target);
    }

    @Override
    public long getStartPointer() {
        return this.recordHeader.getStartDataPointer();
    }

    @Override
    protected void checkConsistency() throws IOException {
    }

    @Override
    protected void write(ByteBuffer buffer) {
        data.flip();
        buffer.put(data);
    }

    @Override
    protected void read(ByteBuffer buffer) {
        data.clear();
        data.put(buffer);
    }

}
