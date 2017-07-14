package hamner.db;

import java.io.IOException;


public class RecordsFile extends BaseRecordsFile {
    /**
     * Creates a new database file.  The initialSize parameter determines the
     * amount of space which is allocated for the index.  The index can grow
     * dynamically, but the parameter is provide to increase
     * efficiency.
     */
    public RecordsFile(String dbPath, int initialSize) throws IOException, RecordsFileException {
        super(dbPath, initialSize);
    }

    /**
     * Opens an existing database and initializes the in-memory index.
     */
    public RecordsFile(String dbPath, String accessFlags) throws IOException, RecordsFileException {
        super(dbPath, accessFlags);
    }


    protected RecordHeader keyToRecordHeader(String lookupKey) throws RecordsFileException, IOException {
        int numRecords = readNumRecordsHeader();
        for (int i = 0; i < numRecords; i++) {
            String key = readKeyFromIndex(i);
            RecordHeader header = readRecordHeaderFromIndex(i);
            header.setIndexPosition(i);
            if (lookupKey.equals(key)) {
                return header;
            }
        }
        return null;
    }


    /**
     * Returns the current number of records in the database.
     */
    public int getNumRecords() {
        return 0;//memIndex.size();
    }

    /**
     * Checks if there is a record belonging to the given key.
     */
    public boolean recordExists(String key) {
        return false;//memIndex.containsKey(key);
    }

    /**
     * This method searches the file for free space and then returns a RecordHeader
     * which uses the space. (O(n) memory accesses)
     */
    protected RecordHeader allocateRecord(String key, int dataLength) throws RecordsFileException, IOException {
        // search for empty space
        RecordHeader newRecord = null;
//      Enumeration e = memIndex.elements();
//      while (e.hasMoreElements())
//      {
//         RecordHeader next = (RecordHeader) e.nextElement();
//         if (dataLength <= next.getFreeSpace())
//         {
//            newRecord = next.split();
//            writeRecordHeaderToIndex(next);
//            break;
//         }
//      }
        if (newRecord == null) {
            // append record to end of file - grows file to allocate space
            long fp = getFileLength();
            setFileLength(fp + dataLength);
            newRecord = new RecordHeader(fp, dataLength);
        }
        return newRecord;
    }

    /**
     * Returns the record to which the target file pointer belongs - meaning the specified location
     * in the file is part of the record data of the RecordHeader which is returned.  Returns null if
     * the location is not part of a record. (O(n) mem accesses)
     */
    protected RecordHeader getRecordAt(long targetFp) throws RecordsFileException {
//      Enumeration e = memIndex.elements();
//      while (e.hasMoreElements())
//      {
//         RecordHeader next = (RecordHeader) e.nextElement();
//         if (targetFp >= next.dataPointer &&
//                 targetFp < next.dataPointer + (long) next.dataCapacity)
//         {
//            return next;
//         }
//      }
        return null;
    }


}






