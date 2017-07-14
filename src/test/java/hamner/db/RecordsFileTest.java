package hamner.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created by dhaa on 14.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordsFileTest {

    private BaseRecordsFile recordsFile;
    private File databaseFile;

    private final static byte[] ONE = "ONE".getBytes();
    private final static byte[] TWO = "TWO".getBytes();
    private final static byte[] THREE = "THREE".getBytes();
    private final static byte[] FOUR = "FOUR".getBytes();
    private final static byte[] FIVE = "FIVE".getBytes();
    private final static byte[] SIX = "SIX".getBytes();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException, RecordsFileException {
        databaseFile = testFolder.newFile("sampleFile.records");
        databaseFile.delete();
        recordsFile = new IndexedRecordsFile(databaseFile.getCanonicalPath(), 64);
    }

    @After
    public void tearDown() throws IOException, RecordsFileException {
        if (recordsFile != null) {
            recordsFile.close();
        }
        if (databaseFile != null) {
            databaseFile.delete();
        }
    }


    @Test
    public void test() throws IOException, RecordsFileException {
        // Given
        //       - index 1
        String key_1 = "1.idx";
        recordsFile.insertRecord(key_1, ONE, 0, ONE.length);
        //       - index 2
        String key_2 = "2.idx";
        recordsFile.insertRecord(key_2, TWO, 0, TWO.length);
        //       - index 3
        String key_3 = "3.idx";
        recordsFile.insertRecord(key_3, THREE, 0, THREE.length);
        //       - index 4
        String key_4 = "4.idx";
        recordsFile.insertRecord(key_4, FOUR, 0, FOUR.length);
        //       - index 5
        String key_5 = "5.idx";
        recordsFile.insertRecord(key_5, FIVE, 0, FIVE.length);
        //       - index 6
        String key_6 = "6.idx";
        recordsFile.insertRecord(key_6, SIX, 0, SIX.length);
        //
        recordsFile.close();
        // When
        RecordReader rr = null;
        BaseRecordsFile secondRecordsFile = new IndexedRecordsFile(databaseFile.getCanonicalPath(), "rw");
        rr = secondRecordsFile.readRecord(key_5);
        byte[] d_5 = rr.getData();
        rr = secondRecordsFile.readRecord(key_1);
        byte[] d_1 = rr.getData();
        // Then
        System.out.println(new String(d_1));
        System.out.println(new String(d_5));
    }

}
