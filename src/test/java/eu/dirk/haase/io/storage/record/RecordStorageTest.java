package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.channel.SeekableInMemoryByteChannel;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 15.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordStorageTest {

    public static final int CAPACITY = 1024 * 10;
    private File file;

    private RecordStorage recordStorage;

    private SeekableInMemoryByteChannel channel;

    private ByteBuffer buffer;

    @Before
    public void setUp() throws IOException {
        byte[] content = new byte[CAPACITY];
        buffer = ByteBuffer.wrap(content);
        channel = new SeekableInMemoryByteChannel(content);
        recordStorage = new RecordStorage(null, channel);
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
        // ===============
        // === Given
        byte[] prolog = new byte[MainHeader.PROLOG.length];
        MainHeader mainHeader = new MainHeader();
        int lastPosition = mainHeader.getLength();
        buffer.limit(lastPosition);
        // ===============
        // === When
        recordStorage.create();
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        //          - Layout of the MainHeader
        assertThat(buffer.getLong()).isEqualTo(0); // => startPointer
        buffer.get(prolog);
        assertThat(prolog).isEqualTo(MainHeader.PROLOG);  // => PROLOG
        assertThat(buffer.getInt()).isEqualTo(1);  // => version
        assertThat(buffer.getInt()).isEqualTo(0);  // => recordCount
        assertThat(buffer.getInt()).isEqualTo(Integer.MIN_VALUE);  // => maxRecordDataLength
        assertThat(buffer.getInt()).isEqualTo(Integer.MAX_VALUE);  // => minRecordDataLength
    }

    @Test
    public void testInsertOneRecord() throws IOException {
        // ===============
        // === Given
        byte[] prolog = new byte[MainHeader.PROLOG.length];
        int dataLength = 123;
        ByteBuffer dataByteBuffer = ByteBuffer.allocate(dataLength);
        MainHeader mainHeader = new MainHeader();
        RecordHeader recordHeader = new RecordHeader();
        int lastPosition = mainHeader.getLength() + recordHeader.getLength();
        recordStorage.create();
        // ===============
        // === When
        int recordIndex = recordStorage.insertRecord(dataByteBuffer, null);
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        //          - Layout of the MainHeader
        assertThat(buffer.getLong()).isEqualTo(0); // => startPointer
        buffer.get(prolog);
        assertThat(prolog).isEqualTo(MainHeader.PROLOG);  // => PROLOG
        assertThat(buffer.getInt()).isEqualTo(1);  // => version
        assertThat(buffer.getInt()).isEqualTo(1);  // => recordCount
        assertThat(buffer.getInt()).isEqualTo(123);  // => maxRecordDataLength
        assertThat(buffer.getInt()).isEqualTo(123);  // => minRecordDataLength
        //       => RecordHeader ------------------
        assertThat(recordIndex).isEqualTo(0);
        //          - Layout of the first RecordHeader
        assertThat(buffer.getLong()).isEqualTo(mainHeader.getLength()); // => startPointer
        assertThat(buffer.getLong()).isEqualTo(recordHeader.getEndPointer()); // => startDataPointer
        assertThat(buffer.getInt()).isEqualTo(dataLength);  // => recordDataCapacity
        assertThat(buffer.getInt()).isEqualTo(dataLength);  // => recordDataLength
        assertThat(buffer.getInt()).isEqualTo(0);  // => recordIndex
        assertThat(buffer.getLong()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

    @Test
    public void testInsertTwoRecord() throws IOException {
        // ===============
        // === Given
        byte[] prolog = new byte[MainHeader.PROLOG.length];
        int dataLength1 = 123;
        int dataLength2 = 23;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        MainHeader mainHeader = new MainHeader();
        RecordHeader recordHeader = new RecordHeader();
        int secondPosition = mainHeader.getLength() + recordHeader.getLength();
        int lastPosition = secondPosition + recordHeader.getLength();
        recordStorage.create();
        // ===============
        // === When
        int recordIndex1 = recordStorage.insertRecord(dataByteBuffer1, key1);
        int recordIndex2 = recordStorage.insertRecord(dataByteBuffer2, key2);
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        //          - Layout of the MainHeader
        assertThat(buffer.getLong()).isEqualTo(0); // => startPointer
        buffer.get(prolog);
        assertThat(prolog).isEqualTo(MainHeader.PROLOG);  // => PROLOG
        assertThat(buffer.getInt()).isEqualTo(1);  // => version
        assertThat(buffer.getInt()).isEqualTo(2);  // => recordCount
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => maxRecordDataLength
        assertThat(buffer.getInt()).isEqualTo(dataLength2);  // => minRecordDataLength
        //          - skip the first RecordHeader
        buffer.position(secondPosition);
        //       => RecordHeader ------------------
        assertThat(recordIndex1).isEqualTo(0);
        assertThat(recordIndex2).isEqualTo(1);
        //          - Layout of the second RecordHeader
        assertThat(buffer.getLong()).isEqualTo(secondPosition); // => startPointer
        assertThat(buffer.getLong()).isEqualTo(lastPosition); // => startDataPointer
        assertThat(buffer.getInt()).isEqualTo(dataLength2);  // => recordDataCapacity
        assertThat(buffer.getInt()).isEqualTo(dataLength2);  // => recordDataLength
        assertThat(buffer.getInt()).isEqualTo(1);  // => recordIndex
        assertThat(buffer.getLong()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

    @Test
    public void testSelectOneRecord() throws IOException {
        // ===============
        // === Given
        byte[] prolog = new byte[MainHeader.PROLOG.length];
        int dataLength1 = 123;
        int dataLength2 = 23;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(dataLength2);
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        MainHeader mainHeader = new MainHeader();
        RecordHeader recordHeader = new RecordHeader();
        int secondPosition = mainHeader.getLength() + recordHeader.getLength();
        int lastPosition = secondPosition + recordHeader.getLength();
        recordStorage.create();
        recordStorage.insertRecord(dataByteBuffer1, key1);
        recordStorage.insertRecord(dataByteBuffer2, key2);
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
        int dataLength = 123;
        ByteBuffer dataByteBuffer = ByteBuffer.allocate(dataLength);
        recordStorage.create();
        recordStorage.insertRecord(dataByteBuffer, null);
        MainHeader mainHeader = recordStorage.getMainHeader();
        RecordHeader firstRecordHeader = new RecordHeader();
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.getStartPointer()).isEqualTo(mainHeader.getLength());
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(firstRecordHeader.getEndPointer());
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength);
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength);
        assertThat(recordHeader.getRecordIndex()).isEqualTo(0);
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(firstRecordHeader.getLastModifiedTimeMillis());
    }


    @Test
    public void testFindLastRecord_WithTwoRecords() throws IOException {
        // ===============
        // === Given
        int dataLength = 123;
        ByteBuffer dataByteBuffer = ByteBuffer.allocate(dataLength);
        recordStorage.create();
        recordStorage.insertRecord(dataByteBuffer, null);
        recordStorage.insertRecord(dataByteBuffer, null);
        RecordHeader secondRecordHeader = new RecordHeader().nextHeader();
        // ===============
        // === When
        RecordHeader recordHeader = recordStorage.findLastRecordHeader();
        // ===============
        // === Then
        assertThat(recordHeader).isNotNull();
        assertThat(recordHeader.getStartPointer()).isEqualTo(secondRecordHeader.getStartPointer());
        assertThat(recordHeader.getStartDataPointer()).isEqualTo(secondRecordHeader.getEndPointer());
        assertThat(recordHeader.getRecordDataCapacity()).isEqualTo(dataLength);
        assertThat(recordHeader.getRecordDataLength()).isEqualTo(dataLength);
        assertThat(recordHeader.getRecordIndex()).isEqualTo(secondRecordHeader.getRecordIndex());
        assertThat(recordHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(secondRecordHeader.getLastModifiedTimeMillis());
    }

}