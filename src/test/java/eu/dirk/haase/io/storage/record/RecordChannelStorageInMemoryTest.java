package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.channel.SeekableInMemoryByteChannel;
import eu.dirk.haase.io.storage.record.data.RecordData;
import eu.dirk.haase.io.storage.record.header.MainHeader;
import eu.dirk.haase.io.storage.record.header.RecordHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 15.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordChannelStorageInMemoryTest extends RecordChannelStorageFileTest {

    public static final int CAPACITY = 1024 * 10;

    private ByteBuffer buffer;

    private byte[] content;

    @Before
    public void setUp() throws IOException {
        content = new byte[CAPACITY];
        buffer = ByteBuffer.wrap(content);
        super.setUp();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        buffer = null;
        content = null;
        super.tearDown();
    }

    @Override
    protected SeekableByteChannel createChannel() {
        return new SeekableInMemoryByteChannel(content);
    }


    @Test
    @Ignore
    @Override
    public void testInitializeRecordStorage() throws IOException {
    }

    @Test
    public void testCreated() throws IOException, InterruptedException {
        // ===============
        // === Given
        byte[] prolog = new byte[MainHeader.PROLOG.length];
        MainHeader mainHeader = new MainHeader();
        int lastPosition = mainHeader.getLength();
        buffer.limit(lastPosition);
        // ===============
        // === When
        recordChannelStorage.create();
        // ===============
        // === Then
        assertThat(channel.position()).isEqualTo(lastPosition);
        //          - Layout of the MainHeader
        assertThat(buffer.getLong()).isEqualTo(21780678656418930L); // => magic data
        assertThat(buffer.getLong()).isEqualTo(0); // => startPointer
        buffer.get(prolog);
        assertThat(prolog).isEqualTo(MainHeader.PROLOG);  // => PROLOG
        assertThat(buffer.getInt()).isEqualTo(1);  // => version
        assertThat(buffer.getInt()).isEqualTo(0);  // => recordCount
        assertThat(buffer.getInt()).isEqualTo(Integer.MIN_VALUE);  // => maxRecordDataLength
        assertThat(buffer.getInt()).isEqualTo(Integer.MAX_VALUE);  // => minRecordDataLength
    }

    @Test
    public void testInsertOneRecord() throws IOException, InterruptedException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        dataByteBuffer1.position(dataLength1);

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
        // ===============
        // === When
        int recordIndex = recordChannelStorage.insertRecord(null, dataByteBuffer1);
        // ===============
        // === Then
        //          - Skip the MainHeader :
        buffer.position(firstPosition);
        //       => RecordHeader ------------------
        assertThat(recordIndex).isEqualTo(0);
        //          - Layout of the first RecordHeader
        assertThat(buffer.getLong()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(buffer.getLong()).isEqualTo(mainHeader.getLength()); // => startPointer
        assertThat(buffer.getLong()).isEqualTo(firstHeader.getEndPointer()); // => startDataPointer
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataCapacity
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataLength
        assertThat(buffer.getInt()).isEqualTo(0);  // => recordIndex
        assertThat(buffer.getLong()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

    @Test
    public void testUpdateOneRecord() throws IOException, InterruptedException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(dataLength1);
        dataByteBuffer1.position(dataLength1);

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

        int recordIndex1a = recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        // ===============
        // === When
        int recordIndex1b = recordChannelStorage.updateRecord(key1, dataByteBuffer1);
        // ===============
        // === Then
        assertThat(recordIndex1a).isEqualTo(0);
        assertThat(recordIndex1b).isEqualTo(1);
        //          - Skip the MainHeader :
        buffer.position(firstPosition);
        //          - skip the first RecordHeader
        buffer.position(secondPosition);
        //       => RecordHeader ------------------
        //          - Layout of the second RecordHeader
        assertThat(buffer.getLong()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(buffer.getLong()).isEqualTo(secondPosition); // => startPointer
        assertThat(buffer.getLong()).isEqualTo(secondDataPosition); // => startDataPointer
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataCapacity
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataLength
        assertThat(buffer.getInt()).isEqualTo(1);  // => recordIndex
        assertThat(buffer.getLong()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

    @Test
    public void testInsertTwoRecord() throws IOException, InterruptedException {
        // ===============
        // === Given
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        int dataLength1 = 123;
        int dataLength2 = 123;
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
        // ===============
        // === When
        int recordIndex1 = recordChannelStorage.insertRecord(key1, dataByteBuffer1);
        int recordIndex2 = recordChannelStorage.insertRecord(key2, dataByteBuffer2);
        // ===============
        // === Then
        //          - Skip the MainHeader :
        buffer.position(firstPosition);
        //          - skip the first RecordHeader
        buffer.position(secondPosition);
        //       => RecordHeader ------------------
        assertThat(recordIndex1).isEqualTo(0);
        assertThat(recordIndex2).isEqualTo(1);
        //          - Layout of the second RecordHeader
        assertThat(buffer.getLong()).isEqualTo(firstHeader.getMagicData()); // => magic data
        assertThat(buffer.getLong()).isEqualTo(secondPosition); // => startPointer
        assertThat(buffer.getLong()).isEqualTo(secondDataPosition); // => startDataPointer
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataCapacity
        assertThat(buffer.getInt()).isEqualTo(dataLength1);  // => recordDataLength
        assertThat(buffer.getInt()).isEqualTo(1);  // => recordIndex
        assertThat(buffer.getLong()).isLessThanOrEqualTo(System.currentTimeMillis()); // => lastModifiedTimeMillis
    }

}