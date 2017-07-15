package eu.dirk.haase.io.file.record;

import java.io.IOException;

/**
 * Created by dhaa on 14.07.17.
 */
public class RecordsFile extends BaseRecordsFile {

    public RecordsFile(String dbPath, String accessFlags) throws IOException {
        super(dbPath, accessFlags);
    }

    public RecordsFile(String dbPath, int initialSize) throws IOException {
        super(dbPath, initialSize);
    }

}
