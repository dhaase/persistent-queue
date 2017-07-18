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
public class RecordChannelStorageFileTest {

    protected RecordChannelStorage recordChannelStorage;
    protected SeekableByteChannel channel;
    private File file;

    @Before
    public void setUp() throws IOException {
        channel = createChannel();
        Shared shared = new Shared();
        recordChannelStorage = new RecordChannelStorage(null, channel);
    }

    protected SeekableByteChannel createChannel() throws IOException {
        file = new File("./RecordChannelStorageFileTest.recordfile.bin");
        file.delete();
        return Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
    }

    @After
    public void tearDown() throws IOException {
        if (file != null) {
            file.delete();
        }
        file = null;
        if (recordChannelStorage != null) {
            recordChannelStorage.close();
        }
        recordChannelStorage = null;
    }

    @Test
    public void testSelectLastRecordHeader() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();

        int firstRecordLengthOverall = recordHeaderLength + recordDatalength + dataLength1;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + firstRecordLengthOverall;
        int secondDataPosition = secondPosition + recordHeaderLength;

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === When
        RecordHeader recordHeader2 = recordChannelStorage.selectRecordHeader(key2);
        // ===============
        // === Then
        //       => RecordHeader ------------------
        assertThat(recordHeader2).isNotNull();
        //          - Layout of the second RecordHeader
        assertThat(recordHeader2.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader2.getRecordDataStartPointer()).isEqualTo(secondDataPosition); // => startDataPointer
        assertThat(recordHeader2.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader2.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader2.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader2.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
        assertThat(recordHeader2.getKey()).isEqualTo(key2); // => key
    }

    @Test
    public void testSelectFirstRecordHeader() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();

        int firstPosition = mainHeaderLength;
        int firstDataPosition = firstPosition + recordHeaderLength;

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === When
        RecordHeader recordHeader1 = recordChannelStorage.selectRecordHeader(key1);
        // ===============
        // === Then
        //       => RecordHeader ------------------
        assertThat(recordHeader1).isNotNull();
        //          - Layout of the first RecordHeader
        assertThat(recordHeader1.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader1.getStartPointer()).isEqualTo(firstPosition); // => startPointer
        assertThat(recordHeader1.getRecordDataStartPointer()).isEqualTo(firstDataPosition); // => startDataPointer
        assertThat(recordHeader1.getRecordDataCapacity()).isEqualTo(dataLength1);  // => recordDataCapacity
        assertThat(recordHeader1.getRecordDataLength()).isEqualTo(dataLength1);  // => recordDataLength
        assertThat(recordHeader1.getRecordIndex()).isEqualTo(0);  // => recordIndex
        assertThat(recordHeader1.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testFindLastRecordHeader_WithNoRecord() throws IOException {
        // Given
        recordChannelStorage.create();
        // When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // Then
        assertThat(recordHeader).isNull();
    }


    @Test
    public void testFindLastRecordHeader_WithOneRecord() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        dataByteBuffer1.position(dataLength1);

        recordChannelStorage.create();
        recordChannelStorage.insertRecord(key1, dataByteBuffer1);

        RecordHeader firstRecordHeader = new RecordHeader();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int mainHeaderLength = mainHeader.getLength();
        int firstPosition = mainHeaderLength;
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        assertThat(recordHeader.getStartPointer()).isEqualTo(firstPosition);
        assertThat(recordHeader.getRecordDataStartPointer()).isEqualTo(firstRecordHeader.getEndPointer());
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength1);
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength1);
        assertThat(recordHeader.getRecordIndex()).isEqualTo(0);
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(firstRecordHeader.getLastModifiedTimeMillis());
    }


    @Test
    public void testFindLastRecordHeader_WithTwoRecords() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();

        int firstRecordLengthOverall = recordHeaderLength + recordDatalength + dataLength1;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + firstRecordLengthOverall;
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        //          - Layout of the second RecordHeader
        assertThat(recordHeader.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader.getRecordDataStartPointer()).isEqualTo(secondPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testFindLastRecordHeader_WithOneRecord_ButDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        dataByteBuffer1.position(dataLength1);

        recordChannelStorage.create();
        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.deleteRecord(key1);
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
    }


    @Test
    public void testFindLastRecordHeader_WithTwoRecords_ButAllDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);

        recordChannelStorage.deleteRecord(key1);
        recordChannelStorage.deleteRecord(key2);
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
    }


    @Test
    public void testFindLastRecordHeader_WithTwoRecords_ButLastDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordOverallLength + dataLength1;

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        recordChannelStorage.deleteRecord(key2);
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isTrue();
        //          - Layout of the second RecordHeader
        assertThat(recordHeader.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader.getRecordDataStartPointer()).isEqualTo(secondPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testFindLastRecordHeader_WithTwoRecords_ButFirstDeleted() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 321;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        dataByteBuffer1.position(dataLength1);
        dataByteBuffer2.position(dataLength2);

        recordChannelStorage.create();

        RecordHeader firstHeader = new RecordHeader();
        RecordData firstData = new RecordData();
        MainHeader mainHeader = recordChannelStorage.getMainHeader();

        int recordDatalength = firstData.getLength();
        int recordHeaderLength = firstHeader.getLength();
        int mainHeaderLength = mainHeader.getLength();
        int recordOverallLength = recordHeaderLength + recordDatalength;

        int firstPosition = mainHeaderLength;
        int secondPosition = firstPosition + recordOverallLength + dataLength1;

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        recordChannelStorage.deleteRecord(key1);
        // ===============
        // === When
        RecordHeader recordHeader = recordChannelStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.isDeleted()).isFalse();
        //          - Layout of the first RecordHeader
        assertThat(recordHeader.getMagicData()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondPosition); // => startPointer
        assertThat(recordHeader.getRecordDataStartPointer()).isEqualTo(secondPosition + recordHeaderLength); // => startDataPointer
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(recordHeader.getRecordIndex()).isEqualTo(1);  // => recordIndex
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }


    @Test
    public void testSelectRecord_WithTwoRecords() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int CAPACITY = 1024;
        byte[] buffer1b = new byte[CAPACITY];
        byte[] buffer2b = new byte[CAPACITY];
        byte[] data1 = "Das ist der erste Record".getBytes();
        byte[] data2 = "Das ist der zweite Record".getBytes();
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(CAPACITY);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(CAPACITY);
        ByteBuffer dataByteBuffer1b = ByteBuffer.wrap(buffer1b);
        ByteBuffer dataByteBuffer2b = ByteBuffer.wrap(buffer2b);
        dataByteBuffer1.put(data1);
        dataByteBuffer2.put(data2);

        recordChannelStorage.create();

        recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === When
        int recordIndex1 = recordChannelStorage.selectRecord(key1, dataByteBuffer1b);
        int recordIndex2 = recordChannelStorage.selectRecord(key2, dataByteBuffer2b);
        // ===============
        // === Then
        assertThat(recordIndex1).isEqualTo(0);
        assertThat(recordIndex2).isEqualTo(1);
        assertThat(buffer1b).startsWith(data1);
        assertThat(buffer2b).startsWith(data2);
    }

    @Test
    public void testInitializeRecordStorage() throws IOException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int CAPACITY = 1024;
        byte[] buffer1a = new byte[CAPACITY];
        byte[] buffer2a = new byte[CAPACITY];
        byte[] buffer1b = new byte[CAPACITY];
        byte[] buffer2b = new byte[CAPACITY];
        byte[] data1 = "Das ist der erste Record".getBytes();
        byte[] data2 = "Das ist der zweite Record".getBytes();
        ByteBuffer dataByteBuffer1a = ByteBuffer.allocate(buffer1a.length);
        ByteBuffer dataByteBuffer2a = ByteBuffer.allocate(buffer2a.length);
        ByteBuffer dataByteBuffer1b = ByteBuffer.wrap(buffer1b);
        ByteBuffer dataByteBuffer2b = ByteBuffer.wrap(buffer2b);
        dataByteBuffer1a.put(data1);
        dataByteBuffer2a.put(data2);

        recordChannelStorage.create();

        recordChannelStorage.insertRecord(key1, dataByteBuffer1a);
        recordChannelStorage.insertRecord(key2, dataByteBuffer2a);
        recordChannelStorage.close();
        recordChannelStorage = null;
        // ===============
        // === When
        Shared shared = new Shared();
        RecordChannelStorage reinitRecordChannelStorage = new RecordChannelStorage(file, StandardOpenOption.READ);
        reinitRecordChannelStorage.initialize();
        int recordIndex1 = reinitRecordChannelStorage.selectRecord(key1, dataByteBuffer1b);
        int recordIndex2 = reinitRecordChannelStorage.selectRecord(key2, dataByteBuffer2b);
        reinitRecordChannelStorage.close();
        // ===============
        // === Then
        assertThat(reinitRecordChannelStorage.openOptions().contains(StandardOpenOption.READ)).isTrue();
        assertThat(reinitRecordChannelStorage.openOptions().contains(StandardOpenOption.CREATE)).isFalse();
        assertThat(recordIndex1).isEqualTo(0);
        assertThat(recordIndex2).isEqualTo(1);
        assertThat(buffer1b).startsWith(data1);
        assertThat(buffer2b).startsWith(data2);
    }

}