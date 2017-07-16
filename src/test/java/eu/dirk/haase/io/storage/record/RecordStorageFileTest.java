package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.data.RecordData;
import eu.dirk.haase.io.storage.record.header.MainHeader;
import eu.dirk.haase.io.storage.record.header.RecordHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 15.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordStorageFileTest {

    protected RecordStorage recordStorage;
    protected SeekableByteChannel channel;
    private File file;

    @Before
    public void setUp() throws IOException {
        channel = createChannel();
        recordStorage = new RecordStorage(null, channel);
    }

    protected SeekableByteChannel createChannel() throws IOException {
        file = new File("./RecordStorageFileTest.recordfile.bin");
        return Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
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
    public void testSelectLastRecord() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordOverallLength;
        int lastPosition = firstPosition + recordOverallLength + recordHeaderLength;

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === When
        RecordHeader recordHeader2 = recordStorage.selectRecordHeader(key2);
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        //       => RecordHeader ------------------
        assertThat(recordHeader2).isNotNull();
        //          - Layout of the second RecordHeader
        assertThat(recordHeader2.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader2.getStartDataPointer()).isEqualTo(lastPosition); // => startDataPointer
        assertThat(recordHeader2.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader2.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader2.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader2.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
        assertThat(recordHeader2.getKey()).isEqualTo(key2); // => key
    }

    @Test
    public void testSelectFirstRecord() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordHeaderLength;
        int lastPosition = firstPosition + recordHeaderLength + recordHeaderLength;

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === When
        RecordHeader recordHeader1 = recordStorage.selectRecordHeader(key1);
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(secondPosition);
        //       => RecordHeader ------------------
        assertThat(recordHeader1).isNotNull();
        //          - Layout of the first RecordHeader
        assertThat(recordHeader1.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader1.getStartPointer()).isEqualTo(firstPosition); // => startPointer
        assertThat(recordHeader1.getStartDataPointer()).isEqualTo(firstPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader1.getRecordDataCapacity()).isEqualTo(dataLength1);  // => recordDataCapacity
        assertThat(recordHeader1.getRecordDataLength()).isEqualTo(dataLength1);  // => recordDataLength
        assertThat(recordHeader1.getRecordIndex()).isEqualTo(0);  // => recordIndex
        assertThat(recordHeader1.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testFindLastRecord_WithNoRecord() throws IOException {
        // Given
        recordStorage.create();
        // When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // Then
        assertThat(recordHeader).isNull();
    }


    @Test
    public void testFindLastRecord_WithOneRecord() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);

        recordStorage.create();
        recordStorage.insertRecord(key1, dataByteBuffer1);

        RecordHeader firstRecordHeader = new RecordHeader();
        MainHeader mainHeader = recordStorage.getMainHeader();

        int mainHeaderLength = mainHeader.getLength();
        int firstPosition = mainHeaderLength;
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        assertThat(recordHeader.getStartPointer()).isEqualTo(firstPosition);
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(firstRecordHeader.getEndPointer());
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength1);
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength1);
        assertThat(recordHeader.getRecordIndex()).isEqualTo(0);
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(firstRecordHeader.getLastModifiedTimeMillis());
    }


    @Test
    public void testFindLastRecord_WithTwoRecords() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);

        RecordHeader secondRecordHeader = new RecordHeader().nextHeader();
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondRecordHeader.getStartPointer());
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(secondRecordHeader.getEndPointer());
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);
        assertThat(recordHeader.getRecordIndex()).isEqualTo(secondRecordHeader.getRecordIndex());
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(secondRecordHeader.getLastModifiedTimeMillis());
    }


    @Test
    public void testFindLastRecord_WithOneRecord_ButDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);

        recordStorage.create();
        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.deleteRecord(key1);
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
    }


    @Test
    public void testFindLastRecord_WithTwoRecords_ButAllDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);

        recordStorage.deleteRecord(key1);
        recordStorage.deleteRecord(key2);
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
    }


    @Test
    public void testFindLastRecord_WithTwoRecords_ButLastDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordOverallLength;
        int lastPosition = firstPosition + recordOverallLength + recordHeaderLength;

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);
        recordStorage.deleteRecord(key2);
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
        //          - Layout of the second RecordHeader
        assertThat(recordHeader.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(secondPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testFindLastRecord_WithTwoRecords_ButFirstDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);

        recordStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordOverallLength;
        int lastPosition = firstPosition + recordOverallLength + recordHeaderLength;

        recordStorage.insertRecord(key1, dataByteBuffer1);
        recordStorage.insertRecord(key2, dataByteBuffer2);
        recordStorage.deleteRecord(key1);
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        //          - Layout of the first RecordHeader
        assertThat(recordHeader.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(secondPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

}