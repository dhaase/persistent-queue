package eu.dirk.haase.io.storage.record.data;

import eu.dirk.haase.io.storage.record.StorageUnit;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by dhaa on 16.07.17.
 */
public class RecordData extends StorageUnit {

    private final static long MAGIC_DATA = StorageUnit.buildMagicData("Data");

    private final static int HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Layout of the AbstractHeader
        headerLength += 8; // size of the magic data
        headerLength += 8; // size of long for startPointer
        headerLength += 4; // size of int for recordDataLength

        HEADER_LENGTH = headerLength;
    }

    /**
     * Magic Data of the Header.
     */
    private long magicData;
    /**
     * Length of the Record-Data (Payload Data).
     */
    private int recordDataLength;
    /**
     * Pointer to the Data.
     */
    private long dataStartPointer;

    public long getDataStartPointer() {
        return dataStartPointer;
    }

    public void setDataStartPointer(long dataStartPointer) {
        this.dataStartPointer = dataStartPointer;
    }

    public void initFromRecordHeader(RecordHeader recordHeader) {
        setStartPointer(recordHeader.getRecordDataStartPointer());
        setRecordDataLength(recordHeader.getRecordDataLength());
        setDataStartPointer(getStartPointer() + HEADER_LENGTH);
    }

    public int getRecordDataLength() {
        return recordDataLength;
    }

    public void setRecordDataLength(int recordDataLength) {
        this.recordDataLength = recordDataLength;
    }

    @Override
    protected void checkConsistency() throws IOException {
    }

    @Override
    public int getLength() {
        return HEADER_LENGTH;
    }

    @Override
    protected void write(ByteBuffer buffer) {
        buffer.putLong(getMagicData());
        buffer.putLong(getStartPointer());
        buffer.putInt(recordDataLength);
    }

    @Override
    protected void read(ByteBuffer buffer) {
        magicData = buffer.getLong();
        setStartPointer(buffer.getLong());
        recordDataLength = buffer.getInt();
    }

    public long getMagicData() {
        return MAGIC_DATA;
    }

    public int writeData(SeekableByteChannel channel, ByteBuffer source) throws IOException {
        // Sets the channel's position to the Data's first byte.
        channel.position(getDataStartPointer());
        // Prepare ByteBuffer
        source.flip();
        // Write the content of the buffer to the channel.
        int bytesWritten = channel.write(source);
        if (bytesWritten != getRecordDataLength()) {
            throw new IOException("Insufficient number of bytes written:" +
                    " Count of bytes currently written "
                    + bytesWritten
                    + ", but expected amount is "
                    + getRecordDataLength());
        }
        return bytesWritten;
    }

    public int readData(SeekableByteChannel channel, ByteBuffer target) throws IOException {
        // Sets the channel's position to the Data's first byte.
        channel.position(getDataStartPointer());
        // Prepare ByteBuffer
        target.clear();
        target.limit(getRecordDataLength());
        // Read the content of the entity, to which this channel is connected, into the buffer.
        int bytesRead = channel.read(target);
        if (bytesRead != getRecordDataLength()) {
            throw new IOException("Insufficient number of bytes read:" +
                    " Count of bytes currently read "
                    + bytesRead
                    + ", but expected amount is "
                    + getRecordDataLength());
        }
        return bytesRead;
    }

}