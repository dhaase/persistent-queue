package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.record.StorageUnit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dhaa on 15.07.17.
 */
final public class MainHeader extends AbstractHeader {

    /**
     * Prolog is a text which describes the origin or purpose of the storages unit (= the file).
     */
    public final static byte[] PROLOG = MainHeader.class.getCanonicalName().getBytes();

    private final static int VERSION = 1;

    private final static int SUB_HEADER_LENGTH;

    private final static long MAGIC_DATA = StorageUnit.buildMagicData("MainHdr");

    static {
        int headerLength = 0;

        // Sub-Layout of the AbstractHeader
        // (first Layout-Part see AbstractHeader.HEADER_LENGTH)
        headerLength += PROLOG.length; // size of byte[] for PROLOG
        headerLength += 4; // size of int for version
        headerLength += 4; // size of int for recordCount
        headerLength += 4; // size of int for maxRecordDataLength
        headerLength += 4; // size of int for minRecordDataLength

        SUB_HEADER_LENGTH = headerLength;
    }

    /**
     * Structure Version of the storage unit.
     */
    private final byte[] prolog;
    /**
     * Structure Version of the storage unit.
     */
    private int version = VERSION;
    /**
     * Count of current blocks in the storage unit.
     */
    private int recordCount;
    /**
     * Maximum length of the blocks in the storage unit.
     */
    private int maxRecordDataLength = Integer.MIN_VALUE;
    /**
     * Minimum length of the blocks in the storage unit.
     */
    private int minRecordDataLength = Integer.MAX_VALUE;


    public MainHeader() {
        super(SUB_HEADER_LENGTH);
        this.prolog = new byte[PROLOG.length];
        setStartPointer(0);
    }

    @Override
    public long getMagicData() {
        return MAGIC_DATA;
    }

    public int getVersion() {
        return version;
    }

    public boolean isCompabible() {
        return (version == VERSION);
    }

    @Override
    public List<String> enlistConsistencyErrors() throws IOException {
        List<String> errorReasonList = super.enlistConsistencyErrors();
        if ((recordCount != 0) && (maxRecordDataLength < minRecordDataLength)) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("maxRecordDataLength can not be less than minRecordDataLength:" +
                    " maxRecordDataLength is currently "
                    + maxRecordDataLength
                    + " and minRecordDataLength is "
                    + minRecordDataLength);
        }
        if (recordCount < 0) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("recordCount can not be below 0:" +
                    " recordCount is currently "
                    + recordCount);

        }
        if (!isCompabible()) {
            errorReasonList = (errorReasonList != null ? errorReasonList : new ArrayList<String>());
            errorReasonList.add("Version is incompatible:" +
                    " Version is currently "
                    + version
                    + " and expected Version is "
                    + VERSION);

        }
        return errorReasonList;
    }

    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.put(PROLOG);
        buffer.putInt(version);
        buffer.putInt(recordCount);
        buffer.putInt(maxRecordDataLength);
        buffer.putInt(minRecordDataLength);
    }

    @Override
    public void read(ByteBuffer buffer) {
        super.read(buffer);
        buffer.get(prolog);
        version = buffer.getInt();
        recordCount = buffer.getInt();
        maxRecordDataLength = buffer.getInt();
        minRecordDataLength = buffer.getInt();
    }

    public void initFromRecordHeader(RecordHeader recordHeader) {
        this.recordCount = recordHeader.getRecordIndex();
        this.maxRecordDataLength = Math.max(recordHeader.getRecordDataLength(), this.maxRecordDataLength);
        this.minRecordDataLength = Math.min(recordHeader.getRecordDataLength(), this.minRecordDataLength);
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public int getMaxRecordDataLength() {
        return maxRecordDataLength;
    }

    public void setMaxRecordDataLength(int maxRecordDataLength) {
        this.maxRecordDataLength = maxRecordDataLength;
    }

    public int getMinRecordDataLength() {
        return minRecordDataLength;
    }

    public void setMinRecordDataLength(int minRecordDataLength) {
        this.minRecordDataLength = minRecordDataLength;
    }

}
