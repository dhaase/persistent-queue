package eu.dirk.haase.io.file.record;

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

    private BaseRecordsFile givenRecordsFile;
    private BaseRecordsFile whenRecordsFile;
    private BaseRecordsFile thenRecordsFile;
    private File databaseFile;

    private final static byte[] ONE_DIGIT = "1".getBytes();
    private final static byte[] ONE_LONG = "--- one ---".getBytes();
    private final static byte[] ONE = "ONE".getBytes();
    private final static byte[] EINS = "EINS".getBytes();

    private final static byte[] TWO_DIGIT = "2".getBytes();
    private final static byte[] TWO_LONG = "--- two ---".getBytes();
    private final static byte[] TWO = "TWO".getBytes();
    private final static byte[] ZWEI = "ZWEI".getBytes();

    private final static byte[] THREE = "THREE".getBytes();
    private final static byte[] DREI = "DREI".getBytes();

    private final static byte[] FOUR = "FOUR".getBytes();
    private final static byte[] VIER = "VIER".getBytes();

    private final static byte[] FIVE = "FIVE".getBytes();
    private final static byte[] FUENF = "FUENF".getBytes();

    private final static byte[] SIX = "SIX".getBytes();
    private final static byte[] SECHS = "SECHS".getBytes();

    private final static String key_1 = "1.idx";
    private final static String key_2 = "2.idx";
    private final static String key_3 = "3.idx";
    private final static String key_4 = "4.idx";
    private final static String key_5 = "5.idx";
    private final static String key_6 = "6.idx";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        databaseFile = testFolder.newFile("sampleFile.records");
        databaseFile.delete();
        givenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), 64);
        givenRecordsFile.addRecord(key_1, ONE, 0, ONE.length);
        givenRecordsFile.addRecord(key_2, TWO, 0, TWO.length);
        givenRecordsFile.addRecord(key_3, THREE, 0, THREE.length);
        givenRecordsFile.addRecord(key_4, FOUR, 0, FOUR.length);
        givenRecordsFile.addRecord(key_5, FIVE, 0, FIVE.length);
        givenRecordsFile.addRecord(key_6, SIX, 0, SIX.length);
    }

    @After
    public void tearDown() throws IOException {
        if (givenRecordsFile != null) {
            givenRecordsFile.close();
        }
        if (whenRecordsFile != null) {
            whenRecordsFile.close();
        }
        if (thenRecordsFile != null) {
            thenRecordsFile.close();
        }
        if (databaseFile != null) {
            databaseFile.delete();
        }
        databaseFile = null;
        givenRecordsFile = null;
        whenRecordsFile = null;
        thenRecordsFile = null;
    }


    @Test
    public void testInsertRecord_after_delete_start_file() throws IOException {
        // Given
        long givenFileLengthFirst = givenRecordsFile.getFileLength();

        givenRecordsFile.deleteRecord(key_1);
        givenRecordsFile.deleteRecord(key_2);
        givenRecordsFile.deleteRecord(key_3);

        long givenFileLengthLast = givenRecordsFile.getFileLength();
        givenRecordsFile.close();
        // When
        whenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");

        whenRecordsFile.addRecord(key_1, EINS, 0, EINS.length);
        whenRecordsFile.addRecord(key_2, ZWEI, 0, ZWEI.length);
        whenRecordsFile.addRecord(key_3, DREI, 0, DREI.length);

        long whenFileLength = whenRecordsFile.getFileLength();
        // Then
        assertThat(givenFileLengthFirst).isEqualTo(givenFileLengthLast);

        byte[] d_1 = new byte[100];
        byte[] d_2 = new byte[100];
        byte[] d_3 = new byte[100];

        thenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "r");

        int len_1 = thenRecordsFile.readRecord(key_1, d_1, 0);
        int len_2 = thenRecordsFile.readRecord(key_2, d_2, 0);
        int len_3 = thenRecordsFile.readRecord(key_3, d_3, 0);

        assertThat(len_1).isEqualTo(EINS.length);
        assertThat(len_2).isEqualTo(ZWEI.length);
        assertThat(len_3).isEqualTo(DREI.length);

        assertThat(d_1).startsWith(EINS);
        assertThat(d_2).startsWith(ZWEI);
        assertThat(d_3).startsWith(DREI);
    }


    @Test
    public void testInsertRecord_after_delete_center_file() throws IOException {
        // Given
        long givenFileLengthFirst = givenRecordsFile.getFileLength();

        givenRecordsFile.deleteRecord(key_3);
        givenRecordsFile.deleteRecord(key_4);
        givenRecordsFile.deleteRecord(key_5);

        long givenFileLengthLast = givenRecordsFile.getFileLength();
        givenRecordsFile.close();
        // When
        whenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");

        whenRecordsFile.addRecord(key_3, DREI, 0, DREI.length);
        whenRecordsFile.addRecord(key_4, VIER, 0, VIER.length);
        whenRecordsFile.addRecord(key_5, FUENF, 0, FUENF.length);

        long whenFileLength = whenRecordsFile.getFileLength();
        // Then
        assertThat(givenFileLengthFirst).isEqualTo(givenFileLengthLast);
        assertThat(whenFileLength).isGreaterThan(givenFileLengthLast);

        byte[] d_3 = new byte[100];
        byte[] d_4 = new byte[100];
        byte[] d_5 = new byte[100];

        thenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "r");

        int len_3 = thenRecordsFile.readRecord(key_3, d_3, 0);
        int len_4 = thenRecordsFile.readRecord(key_4, d_4, 0);
        int len_5 = thenRecordsFile.readRecord(key_5, d_5, 0);

        assertThat(len_3).isEqualTo(DREI.length);
        assertThat(len_4).isEqualTo(VIER.length);
        assertThat(len_5).isEqualTo(FUENF.length);

        assertThat(d_3).startsWith(DREI);
        assertThat(d_4).startsWith(VIER);
        assertThat(d_5).startsWith(FUENF);
    }


    @Test
    public void testInsertRecord_after_delete_end_file() throws IOException {
        // Given
        long givenFileLengthFirst = givenRecordsFile.getFileLength();

        givenRecordsFile.deleteRecord(key_6);
        givenRecordsFile.deleteRecord(key_5);
        givenRecordsFile.deleteRecord(key_4);

        long givenFileLengthLast = givenRecordsFile.getFileLength();
        // When
        whenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");

        whenRecordsFile.addRecord(key_4, VIER, 0, VIER.length);
        whenRecordsFile.addRecord(key_5, FUENF, 0, FUENF.length);
        whenRecordsFile.addRecord(key_6, SECHS, 0, SECHS.length);

        long whenFileLength = whenRecordsFile.getFileLength();
        // Then
        assertThat(givenFileLengthLast).isLessThan(givenFileLengthFirst);
        assertThat(givenFileLengthFirst).isLessThan(whenFileLength);

        byte[] d_4 = new byte[100];
        byte[] d_5 = new byte[100];
        byte[] d_6 = new byte[100];

        thenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "r");

        int len_4 = thenRecordsFile.readRecord(key_4, d_4, 0);
        int len_5 = thenRecordsFile.readRecord(key_5, d_5, 0);
        int len_6 = thenRecordsFile.readRecord(key_6, d_6, 0);

        assertThat(len_4).isEqualTo(VIER.length);
        assertThat(len_5).isEqualTo(FUENF.length);
        assertThat(len_6).isEqualTo(SECHS.length);

        assertThat(d_4).startsWith(VIER);
        assertThat(d_5).startsWith(FUENF);
        assertThat(d_6).startsWith(SECHS);
    }


    @Test
    public void testUpdateRecord_inplace() throws IOException {
        // Given
        long givenFileLength = givenRecordsFile.getFileLength();
        // When
        whenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");

        whenRecordsFile.updateRecord(key_1, ONE_DIGIT, 0, ONE_DIGIT.length);
        whenRecordsFile.updateRecord(key_2, TWO_DIGIT, 0, TWO_DIGIT.length);

        long whenFileLength = whenRecordsFile.getFileLength();
        whenRecordsFile.close();
        // Then
        assertThat(whenFileLength).isEqualTo(givenFileLength);

        byte[] d_1 = new byte[100];
        byte[] d_2 = new byte[100];

        thenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "r");

        int len_1 = thenRecordsFile.readRecord(key_1, d_1, 0);
        int len_2 = thenRecordsFile.readRecord(key_2, d_2, 0);

        assertThat(len_1).isEqualTo(ONE_DIGIT.length);
        assertThat(len_2).isEqualTo(TWO_DIGIT.length);

        assertThat(d_1).startsWith(ONE_DIGIT);
        assertThat(d_2).startsWith(TWO_DIGIT);
    }


    @Test
    public void testUpdateRecord_expanding() throws IOException {
        // Given
        long givenFileLength = givenRecordsFile.getFileLength();
        // When
        whenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "rw");

        whenRecordsFile.updateRecord(key_1, ONE_LONG, 0, ONE_LONG.length);
        whenRecordsFile.updateRecord(key_2, TWO_LONG, 0, TWO_LONG.length);

        long whenFileLength = whenRecordsFile.getFileLength();
        whenRecordsFile.close();
        // Then
        assertThat(whenFileLength).isGreaterThan(givenFileLength);

        byte[] d_1 = new byte[100];
        byte[] d_2 = new byte[100];

        thenRecordsFile = new RecordsFile(databaseFile.getCanonicalPath(), "r");

        int len_1 = thenRecordsFile.readRecord(key_1, d_1, 0);
        int len_2 = thenRecordsFile.readRecord(key_2, d_2, 0);

        System.out.println(new String(d_1, 0, 3));

        assertThat(len_1).isEqualTo(ONE_LONG.length);
        assertThat(len_2).isEqualTo(TWO_LONG.length);

        assertThat(d_1).startsWith(ONE_LONG);
        assertThat(d_2).startsWith(TWO_LONG);
    }


}
