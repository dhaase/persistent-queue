package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.header.RecordHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 15.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordStorageTest {

    private File file;

    private RecordStorage recordStorage;

    @Before
    public void setUp() throws IOException {
        file = new File("RecordStorageTest.test.bin");
        recordStorage = new RecordStorage(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        recordStorage.create();
    }

    @After
    public void tearDown() throws IOException {
        if (file != null) {
            file.delete();
        }
        file = null;
        if (recordStorage != null) {
            recordStorage.close();
        }
        recordStorage = null;
    }

    @Test
    public void testCreated() throws IOException {
        // Given
        // When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // Then
        assertThat(recordHeader).isNull();
    }

    @Test
    public void testAddedOne() throws IOException {
        // Given
        // When
        int recordIndex = recordStorage.addRecord(null, null);
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordIndex).isEqualTo(1);
        assertThat(recordIndex).isEqualTo(recordHeader.getRecordIndex() - 1);
    }

    @Test
    public void testAddedTwo() throws IOException {
        // Given
        // When
        int recordIndex1 = recordStorage.addRecord(null, null);
        int recordIndex2 = recordStorage.addRecord(null, null);
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordIndex1).isEqualTo(1);
        assertThat(recordIndex2).isEqualTo(2);
        assertThat(recordIndex2).isEqualTo(recordHeader.getRecordIndex() - 1);
    }


}