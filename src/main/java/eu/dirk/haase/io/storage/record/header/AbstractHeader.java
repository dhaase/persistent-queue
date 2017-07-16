package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.channel.ChannelAwareUnit;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dhaa on 15.07.17.
 */
public abstract class AbstractHeader extends ChannelAwareUnit {

    private final static int HEADER_LENGTH;

    static {
        int headerLength = 0;

        // Layout of the AbstractHeader
        headerLength += 8; // size of the magic data
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
    /**
     * Magic Data of the Header.
     */
    private long magicData;


    protected AbstractHeader(int subHeaderLength) {
        this.headerLength = HEADER_LENGTH + subHeaderLength;
    }

    static long buildMagicData(String magicStr) {
        // The input array is assumed to be in big-endian byte-order:
        // the most significant byte is in the zeroth element.
        return new BigInteger(magicStr.getBytes()).longValue();
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
    public boolean isValid() {
        return super.isValid() && (magicData == getMagicData());
    }

    @Override
    protected void write(ByteBuffer buffer) {
        buffer.putLong(getMagicData());
        buffer.putLong(startPointer);
    }

    @Override
    protected void read(ByteBuffer buffer) {
        magicData = buffer.getLong();
        startPointer = buffer.getLong();
    }

    @Override
    protected void checkConsistency() throws IOException {
        List<String> errorReasonList = enlistConsistencyErrors();
        if ((errorReasonList != null) && !errorReasonList.isEmpty()) {
            throw new IOException(errorReasonList.toString());
        }
    }

    protected abstract long getMagicData();

    public List<String> enlistConsistencyErrors() throws IOException {
        List<String> errorReasonList = null;
        if (startPointer < 0) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("startPointer can not be below 0: startPointer is currently "
                    + startPointer);

        }
        return errorReasonList;
    }
}
