package hamner.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class IndexedRecordsFile extends BaseRecordsFile {

    /**
     * Hashtable which holds the in-memory index. For efficiency, the entire index
     * is cached in memory. The hashtable maps a key of type String to a RecordHeader.
     */
    protected final Map<String, RecordHeader> memIndex;

    /**
     * Creates a new database file.  The initialSize parameter determines the
     * amount of space which is allocated for the index.  The index can grow
     * dynamically, but the parameter is provide to increase
     * efficiency.
     */
    public IndexedRecordsFile(String dbPath, int initialSize) throws IOException {
        super(dbPath, initialSize);
        memIndex = new HashMap<String, RecordHeader>();
    }

    /**
     * Opens an existing database and initializes the in-memory index.
     */
    public IndexedRecordsFile(String dbPath, String accessFlags) throws IOException {
        super(dbPath, accessFlags);
        Iterator<RecordHeader> iterator = recordHeaderIterator();
        memIndex = new HashMap<String, RecordHeader>();
        while (iterator.hasNext()) {
            RecordHeader header = iterator.next();
            memIndex.put(header.getKey(), header);
        }
    }

    /**
     * Returns the current number of records in the database.
     */
    public int getNumRecords() {
        return memIndex.size();
    }

    /**
     * Checks if there is a record belonging to the given key.
     */
    public boolean recordExists(String key) {
        return memIndex.containsKey(key);
    }

    /**
     * Maps a key to a record header by looking it up in the in-memory index.
     */
    protected RecordHeader keyToRecordHeader(String key) throws IOException {
        RecordHeader h = (RecordHeader) memIndex.get(key);
        if (h == null) {
            throw new IOException("Key not found: " + key);
        }
        return h;
    }

    /**
     * This method searches the file for free space and then returns a RecordHeader
     * which uses the space. (O(n) memory accesses)
     */
    protected RecordHeader allocateRecord(String key, int dataLength) throws IOException {
        // search for empty space
        RecordHeader newRecord = null;
        Iterator<RecordHeader> e = memIndex.values().iterator();
        while (e.hasNext()) {
            RecordHeader next = e.next();
            if (dataLength <= next.getFreeSpace()) {
                newRecord = next.split();
                newRecord.setKey(key);
                writeRecordHeaderToIndex(next);
                break;
            }
        }
        if (newRecord == null) {
            return super.allocateRecord(key, dataLength);
        }
        return newRecord;
    }

    /**
     * Returns the record to which the target file pointer belongs - meaning the specified location
     * in the file is part of the record data of the RecordHeader which is returned.  Returns null if
     * the location is not part of a record. (O(n) mem accesses)
     */
    protected RecordHeader findRecordHeaderAt(long targetFp) throws IOException {
        Iterator<RecordHeader> e = memIndex.values().iterator();
        while (e.hasNext()) {
            RecordHeader next = e.next();
            if ((targetFp >= next.dataPointer) && (targetFp < (next.dataPointer + (long) next.dataCapacity))) {
                return next;
            }
        }
        return null;
    }


    /**
     * Closes the database.
     */
    public void close() throws IOException, IOException {
        try {
            super.close();
        } finally {
            memIndex.clear();
        }
    }

    /**
     * Adds the new record to the in-memory index and calls the super class add
     * the index entry to the file.
     */
    protected void addEntryToIndex(String key, RecordHeader newRecord, int currentNumRecords) throws IOException {
        super.addEntryToIndex(key, newRecord, currentNumRecords);
        memIndex.put(key, newRecord);
    }

    /**
     * Removes the record from the index. Replaces the target with the entry at the
     * end of the index.
     */
    protected void deleteEntryFromIndex(String key, RecordHeader header, int currentNumRecords) throws IOException {
        super.deleteEntryFromIndex(key, header, currentNumRecords);
        memIndex.remove(key);
    }


}






