package hamner.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

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
    private final static byte[] DREI = "DREI".getBytes();
    private final static byte[] FOUR = "FOUR".getBytes();
    private final static byte[] FIVE = "FIVE".getBytes();
    private final static byte[] SIX = "SIX".getBytes();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        databaseFile = testFolder.newFile("sampleFile.records");
        databaseFile.delete();
        recordsFile = new RecordsFile(databaseFile.getCanonicalPath(), 64);
    }

    @After
    public void tearDown() throws IOException {
        if (recordsFile != null) {
            recordsFile.close();
        }
        if (databaseFile != null) {
            databaseFile.delete();
        }
    }


    @Test
    public void testInsertRecord() throws IOException {
        // Given
        //       - index 1
        String key_1 = "1.idx";
        recordsFile.addRecord(key_1, ONE, 0, ONE.length);
        //       - index 2
        String key_2 = "2.idx";
        recordsFile.addRecord(key_2, TWO, 0, TWO.length);
        //       - index 3
        String key_3 = "3.idx";
        recordsFile.addRecord(key_3, THREE, 0, THREE.length);
        //       - index 4
        String key_4 = "4.idx";
        recordsFile.addRecord(key_4, FOUR, 0, FOUR.length);
        //       - index 5
        String key_5 = "5.idx";
        recordsFile.addRecord(key_5, FIVE, 0, FIVE.length);
        //       - index 6
        String key_6 = "6.idx";
        recordsFile.addRecord(key_6, SIX, 0, SIX.length);
        //
        recordsFile.close();
        // When
        BaseRecordsFile secondRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");
        byte[] d_5 = new byte[100];
        byte[] d_1 = new byte[100];
        int len_5 = secondRecordsFile.readRecord(key_5, d_5, 0);
        int len_1 = secondRecordsFile.readRecord(key_1, d_1, 0);
        // Then
        System.out.println(new String(d_1, 0, len_1));
        System.out.println(new String(d_5, 0, len_5));
    }


    @Test
    public void testUpdateRecord() throws IOException {
        // Given
        //       - index 1
        String key_1 = "1.idx";
        recordsFile.addRecord(key_1, ONE, 0, ONE.length);
        //       - index 2
        String key_2 = "2.idx";
        recordsFile.addRecord(key_2, TWO, 0, TWO.length);
        //       - index 3
        String key_3 = "3.idx";
        recordsFile.addRecord(key_3, THREE, 0, THREE.length);
        //       - index 4
        String key_4 = "4.idx";
        recordsFile.addRecord(key_4, FOUR, 0, FOUR.length);
        //       - index 5
        String key_5 = "5.idx";
        recordsFile.addRecord(key_5, FIVE, 0, FIVE.length);
        //       - index 6
        String key_6 = "6.idx";
        recordsFile.addRecord(key_6, SIX, 0, SIX.length);
        //
        recordsFile.close();
        // When
        BaseRecordsFile secondRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");
        byte[] d_3a = new byte[100];
        byte[] d_3b = new byte[100];
        int len_3a = secondRecordsFile.readRecord(key_3, d_3a, 0);
        secondRecordsFile.updateRecord(key_3, DREI, 0, DREI.length);
        int len_3b = secondRecordsFile.readRecord(key_3, d_3b, 0);
        // Then
        System.out.println(new String(d_3a, 0, len_3a));
        System.out.println(new String(d_3b, 0, len_3b));
    }


    @Test
    public void testDeleteRecord_center_record() throws IOException {
        // Given
        //       - index 1
        String key_1 = "1.idx";
        recordsFile.addRecord(key_1, ONE, 0, ONE.length);
        //       - index 2
        String key_2 = "2.idx";
        recordsFile.addRecord(key_2, TWO, 0, TWO.length);
        //       - index 3
        String key_3 = "3.idx";
        recordsFile.addRecord(key_3, THREE, 0, THREE.length);
        //       - index 4
        String key_4 = "4.idx";
        recordsFile.addRecord(key_4, FOUR, 0, FOUR.length);
        //       - index 5
        String key_5 = "5.idx";
        recordsFile.addRecord(key_5, FIVE, 0, FIVE.length);
        //       - index 6
        String key_6 = "6.idx";
        recordsFile.addRecord(key_6, SIX, 0, SIX.length);
        long initialFileSize = databaseFile.length();
        //
        recordsFile.close();
        // When
        BaseRecordsFile secondRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");
        int count1 = secondRecordsFile.getNumRecords();
        long fileLength1 = secondRecordsFile.getFileLength();
        secondRecordsFile.deleteRecord(key_3);
        int count2 = secondRecordsFile.getNumRecords();
        long fileLength2 = secondRecordsFile.getFileLength();
        // Then
        assertThat(count1).isEqualTo(6);
        assertThat(count2).isEqualTo(5);
        assertThat(fileLength1).isEqualTo(initialFileSize);
        assertThat(fileLength2).isEqualTo(initialFileSize);
        assertThat(fileLength2).isEqualTo(databaseFile.length());
    }


    @Test
    public void testDeleteRecord_last_record() throws IOException {
        // Given
        //       - index 1
        String key_1 = "1.idx";
        recordsFile.addRecord(key_1, ONE, 0, ONE.length);
        //       - index 2
        String key_2 = "2.idx";
        recordsFile.addRecord(key_2, TWO, 0, TWO.length);
        //       - index 3
        String key_3 = "3.idx";
        recordsFile.addRecord(key_3, THREE, 0, THREE.length);
        //       - index 4
        String key_4 = "4.idx";
        recordsFile.addRecord(key_4, FOUR, 0, FOUR.length);
        //       - index 5
        String key_5 = "5.idx";
        recordsFile.addRecord(key_5, FIVE, 0, FIVE.length);
        //       - index 6
        String key_6 = "6.idx";
        recordsFile.addRecord(key_6, SIX, 0, SIX.length);
        long initialFileSize = databaseFile.length();
        //
        recordsFile.close();
        // When
        BaseRecordsFile secondRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");
        int count1 = secondRecordsFile.getNumRecords();
        long fileLength1 = secondRecordsFile.getFileLength();
        secondRecordsFile.deleteRecord(key_6);
        int count2 = secondRecordsFile.getNumRecords();
        long fileLength2 = secondRecordsFile.getFileLength();
        // Then
        assertThat(count1).isEqualTo(6);
        assertThat(count2).isEqualTo(5);
        assertThat(fileLength1).isEqualTo(initialFileSize);
        assertThat(fileLength2).isEqualTo(initialFileSize - SIX.length);
        assertThat(fileLength2).isEqualTo(databaseFile.length());
    }

}
