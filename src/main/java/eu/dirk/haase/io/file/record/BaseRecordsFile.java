package eu.dirk.haase.io.file.record;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public abstract class BaseRecordsFile {

    // Total length in bytes of the global database headers.
    private static final int FILE_HEADERS_REGION_LENGTH = 16;
    // Number of bytes in the record header.
    private static final int RECORD_HEADER_LENGTH = 16;
    // The length of a key in the index.
    private static final int MAX_KEY_LENGTH = 64;
    // The total length of one index entry - the key length plus the record header length.
    private static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH + RECORD_HEADER_LENGTH;
    // File pointer to the num records header.
    private static final long NUM_RECORDS_HEADER_LOCATION = 0;
    // File pointer to the data start pointer header.
    private static final long DATA_START_HEADER_LOCATION = 4;
    // Current file pointer to the start of the record data.
    private long dataStartPtr;
    // The database file.
    private RandomAccessFile file;
    // Count of current records.
    private int numRecords;

    /**
     * Creates a new database file, initializing the appropriate headers. Enough space is allocated in
     * the index for the specified initial size.
     */
    protected BaseRecordsFile(String dbPath, int initialSize) throws IOException {
        File f = new File(dbPath);
        if (f.exists()) {
            throw new IOException("Database already exits: " + dbPath);
        }
        this.file = new RandomAccessFile(f, "rw");
        this.dataStartPtr = indexPositionToKeyFp(initialSize);  // Record Data Region starts were the
        setFileLength(dataStartPtr);                       // (i+1)th index entry would start.
        writeNumRecordsHeader(0);
        writeDataStartPtrHeader(dataStartPtr);
    }

    /**
     * Opens an existing database file and initializes the dataStartPtr. The accessFlags
     * parameter can be "r" or "rw" -- as defined in RandomAccessFile.
     */
    protected BaseRecordsFile(String dbPath, String accessFlags) throws IOException {
        File f = new File(dbPath);
        if (!f.exists()) {
            throw new IOException("Database not found: " + dbPath);
        }
        this.file = new RandomAccessFile(f, accessFlags);
        this.dataStartPtr = readDataStartHeader();
        this.numRecords = readNumRecordsHeader();
    }

    /**
     * Returns the number or records in the database.
     */
    public int getNumRecords() {
        return numRecords;
    }

    /**
     * Checks there is a record with the given key.
     */
    public boolean recordExists(String key) throws IOException {
        return (keyToRecordHeader(key) != null);
    }

    /**
     * Locates space for a new record of dataLength size and initializes a RecordHeader.
     */
    protected RecordHeader allocateRecord(String key, int dataLength) throws IOException {
        // append record to end of file - grows file to allocate space
        long fp = getFileLength();
        setFileLength(fp + dataLength);
        return new RecordHeader(key, fp, dataLength);
    }

    /**
     * Returns the record to which the target file pointer belongs - meaning the specified location
     * in the file is part of the record data of the RecordHeader which is returned.  Returns null if
     * the location is not part of a record. (O(n) mem accesses)
     */
    protected RecordHeader findRecordHeaderAt(long targetFp) throws IOException {
        Iterator<RecordHeader> e = recordHeaderIterator();
        while (e.hasNext()) {
            RecordHeader next = e.next();
            if ((targetFp >= next.dataPointer) && (targetFp < (next.dataPointer + (long) next.dataCapacity))) {
                return next;
            }
        }
        return null;
    }

    protected long getFileLength() throws IOException {
        return file.length();
    }

    protected void setFileLength(long l) throws IOException {
        file.setLength(l);
    }

    /**
     * Reads the number of records header from the file.
     */
    protected int readNumRecordsHeader() throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        return file.readInt();
    }

    /**
     * Writes the number of records header to the file.
     */
    protected void writeNumRecordsHeader(int numRecords) throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        file.writeInt(numRecords);
    }

    /**
     * Reads the data start pointer header from the file.
     */
    protected long readDataStartHeader() throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        return file.readLong();
    }

    /**
     * Writes the data start pointer header to the file.
     */
    protected void writeDataStartPtrHeader(long dataStartPtr) throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        file.writeLong(dataStartPtr);
    }


    /**
     * Returns a file pointer in the index pointing to the first byte
     * in the key located at the given index position.
     */
    protected long indexPositionToKeyFp(int pos) {
        return FILE_HEADERS_REGION_LENGTH + (INDEX_ENTRY_LENGTH * pos);
    }

    /**
     * Returns a file pointer in the index pointing to the first byte
     * in the record pointer located at the given index position.
     */
    long indexPositionToRecordHeaderFp(int pos) {
        return indexPositionToKeyFp(pos) + MAX_KEY_LENGTH;
    }

    /**
     * Reads the ith key from the index.
     */
    protected String readKeyFromIndex(int position) {
        try {
            file.seek(indexPositionToKeyFp(position));
            return file.readUTF();
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }

    /**
     * Reads the ith record header from the index.
     */
    protected RecordHeader readRecordHeaderFromIndex(int position) {
        try {
            String key = readKeyFromIndex(position);
            file.seek(indexPositionToRecordHeaderFp(position));
            RecordHeader header = new RecordHeader(key, position);
            header.read(file);
            return header;
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }

    /**
     * Writes the ith record header to the index.
     */
    protected void writeRecordHeaderToIndex(RecordHeader header) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(header.indexPosition));
        header.write(file);
    }

    /**
     * Appends an entry to end of index. Assumes that ensureIndexSpace() has already been called.
     */
    protected void addEntryToIndex(String key, RecordHeader newRecord, int currentNumRecords) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(MAX_KEY_LENGTH);
        (new DataOutputStream(baos)).writeUTF(key);
        if (baos.size() > MAX_KEY_LENGTH) {
            throw new IOException("Key is larger than permitted size of " + MAX_KEY_LENGTH + " bytes");
        }
        file.seek(indexPositionToKeyFp(currentNumRecords));
        byte[] data = baos.toByteArray();
        int l = baos.size();
        file.write(data, 0, l);
        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        newRecord.write(file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords + 1);
    }


    /**
     * Removes the record from the index. Replaces the target with the entry at the
     * end of the index.
     */
    protected void deleteEntryFromIndex(String key, RecordHeader header, int currentNumRecords) throws IOException {
        if (header.indexPosition != currentNumRecords - 1) {
            String lastKey = readKeyFromIndex(currentNumRecords - 1);
            RecordHeader last = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);
            file.seek(indexPositionToKeyFp(last.indexPosition));
            file.writeUTF(lastKey);
            file.seek(indexPositionToRecordHeaderFp(last.indexPosition));
            last.write(file);
        }
        writeNumRecordsHeader(currentNumRecords - 1);
    }

    /**
     * Adds the given record to the database.
     */
    public void addRecord(String key, byte[] data, int offset, int length) throws IOException {
        if (recordExists(key)) {
            throw new IOException("Key exists: " + key);
        }
        ensureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(key, length);
        writeRecordData(newRecord, data, offset, length);
        addEntryToIndex(key, newRecord, getNumRecords());
        ++numRecords;
    }

    /**
     * Updates an existing record. If the new contents do not fit in the original record,
     * then the update is handled by deleting the old record and adding the new.
     */
    public void updateRecord(String key, byte[] data, int offset, int length) throws IOException {
        RecordHeader header = keyToRecordHeader(key);
        if ((length - offset) > header.dataCapacity) {
            deleteRecord(key);
            addRecord(key, data, offset, length);
        } else {
            writeRecordData(header, data, offset, length);
            writeRecordHeaderToIndex(header);
        }
    }

    /**
     * Reads the data for the record with the given key.
     */
    public int readRecord(String key, byte[] data, int offset) throws IOException {
        RecordHeader header = keyToRecordHeader(key);
        return readRecordData(header, data, offset, header.dataCount);
    }

    /**
     * Reads the record data for the given record header.
     */
    protected int readRecordData(RecordHeader header, byte[] data, int offset, int length) throws IOException {
        file.seek(header.dataPointer);
        file.readFully(data, offset, length);
        return (length - offset);
    }


    /**
     * Updates the contents of the given record. A RecordsFileException is thrown if the new data does not
     * fit in the space allocated to the record. The header's data count is updated, but not
     * written to the file.
     */
    protected void writeRecordData(RecordHeader header, byte[] data, int offset, int length) throws IOException {
        if (data.length > header.dataCapacity) {
            throw new IOException("Record data does not fit");
        }
        header.dataCount = data.length;
        file.seek(header.dataPointer);
        file.write(data, offset, length);
    }

    protected RecordHeader keyToRecordHeader(String key) throws IOException {
        Iterator<RecordHeader> e = recordHeaderIterator();
        while (e.hasNext()) {
            RecordHeader next = e.next();
            if (key.equals(next.getKey())) {
                return next;
            }
        }
        return null;
    }

    /**
     * Deletes a record.
     */
    public void deleteRecord(String key) {
        try {
            RecordHeader delRecHead = keyToRecordHeader(key);
            int currentNumRecords = getNumRecords();
            if (getFileLength() == delRecHead.dataPointer + delRecHead.dataCapacity) {
                // shrink file since this is the last record in the file
                setFileLength(delRecHead.dataPointer);
            } else {
                RecordHeader previous = findRecordHeaderAt(delRecHead.dataPointer - 1);
                if (previous != null) {
                    // append space of deleted record onto previous record
                    previous.dataCapacity += delRecHead.dataCapacity;
                    writeRecordHeaderToIndex(previous);
                } else {
                    // target record is first in the file and is deleted by adding its space to
                    // the second record.
                    RecordHeader secondRecord = findRecordHeaderAt(delRecHead.dataPointer + (long) delRecHead.dataCapacity);
                    byte[] data = new byte[secondRecord.dataCount];
                    readRecordData(secondRecord, data, 0, data.length);
                    secondRecord.dataPointer = delRecHead.dataPointer;
                    secondRecord.dataCapacity += delRecHead.dataCapacity;
                    writeRecordData(secondRecord, data, 0, data.length);
                    writeRecordHeaderToIndex(secondRecord);
                }
            }
            deleteEntryFromIndex(key, delRecHead, currentNumRecords);
            --numRecords;
        } catch (Exception ex) {
            throw new RuntimeIOException(ex.toString(), ex);
        }
    }


    // Checks to see if there is space for and additional index entry. If
    // not, space is created by moving records to the end of the file.
    protected void ensureIndexSpace(int requiredNumRecords) throws IOException {
        int currentNumRecords = getNumRecords();
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if (endIndexPtr > getFileLength() && currentNumRecords == 0) {
            setFileLength(endIndexPtr);
            dataStartPtr = endIndexPtr;
            writeDataStartPtrHeader(dataStartPtr);
            return;
        }
        while (endIndexPtr > dataStartPtr) {
            RecordHeader first = findRecordHeaderAt(dataStartPtr);
            byte[] data = new byte[first.dataCount];
            readRecordData(first, data, 0, data.length);
            first.dataPointer = getFileLength();
            first.dataCapacity = data.length;
            setFileLength(first.dataPointer + data.length);
            writeRecordData(first, data, 0, data.length);
            writeRecordHeaderToIndex(first);
            dataStartPtr += first.dataCapacity;
            writeDataStartPtrHeader(dataStartPtr);
        }
    }

    /**
     * Closes the file.
     */
    public void close() throws IOException {
        try {
            if (file != null) {
                file.close();
            }
        } finally {
            file = null;
        }
    }


    public RecordHeaderIterator recordHeaderIterator() {
        return new RecordHeaderIterator();
    }


    class RecordHeaderIterator implements Iterator<RecordHeader> {

        int currRecordIndex;
        int nextRecordIndex;

        public boolean hasNext() {
            return nextRecordIndex < BaseRecordsFile.this.numRecords;
        }

        public RecordHeader next() {
            currRecordIndex = nextRecordIndex;
            RecordHeader rh = readRecordHeaderFromIndex(currRecordIndex);
            ++nextRecordIndex;
            return rh;
        }

        public void remove() {
            String key = readKeyFromIndex(currRecordIndex);
            deleteRecord(key);
        }
    }

}











