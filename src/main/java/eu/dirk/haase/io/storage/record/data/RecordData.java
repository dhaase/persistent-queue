package eu.dirk.haase.io.storage.record.data;

import eu.dirk.haase.io.storage.record.StorageUnit;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

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

    public void initFromRecordHeader(RecordHeader recordHeader) {
        setStartPointer(recordHeader.getStartDataPointer());
        setRecordDataLength(recordHeader.getRecordDataLength());
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
        return HEADER_LENGTH;//recordDataLength;
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

}
