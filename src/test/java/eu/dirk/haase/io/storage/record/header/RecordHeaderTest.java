package eu.dirk.haase.io.storage.record.header;

import eu.dirk.haase.io.storage.channel.SeekableInMemoryByteChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 14.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordHeaderTest {

    private SeekableByteChannel channel1;
    private SeekableByteChannel channel2;
    private File file;

    @After
    public void tearDown() throws IOException {
        if (file != null) {
            file.delete();
        }
        if (channel1 != null) {
            channel1.close();
        }
        if (channel2 != null) {
            channel2.close();
        }
        file = null;
        channel1 = null;
        channel2 = null;
    }


    @Test
    public void testRecordHeader_FirstHeader_Values() {
        // ============
        // Given
        RecordHeader firstHeader = new RecordHeader();
        // ============
        // When
        // ============
        // Then
        assertThat(firstHeader.getStartPointer()).isEqualTo(81L);
        assertThat(firstHeader.getStartDataPointer()).isEqualTo(165L);
        assertThat(firstHeader.getEndPointer()).isEqualTo(165L);
        assertThat(firstHeader.getLength()).isEqualTo(84);
        assertThat(firstHeader.getRecordDataCapacity()).isEqualTo(0);
        assertThat(firstHeader.getRecordDataLength()).isEqualTo(0);
        assertThat(firstHeader.getRecordIndex()).isEqualTo(0);
    }


    @Test
    public void testRecordHeader_FirstHeader_Values_With_Data() {
        // ============
        // Given
        RecordHeader firstHeader = new RecordHeader();
        // ============
        // When
        firstHeader.setRecordDataLength(123);
        firstHeader.setRecordDataCapacity(200);
        // ============
        // Then
        assertThat(firstHeader.getStartPointer()).isEqualTo(81L);
        assertThat(firstHeader.getStartDataPointer()).isEqualTo(165L);
        assertThat(firstHeader.getEndPointer()).isEqualTo(165L);
        assertThat(firstHeader.getLength()).isEqualTo(84);
        assertThat(firstHeader.getRecordDataCapacity()).isEqualTo(200);
        assertThat(firstHeader.getRecordDataLength()).isEqualTo(123);
        assertThat(firstHeader.getRecordIndex()).isEqualTo(0);
    }


    @Test
    public void testNextRecordHeader() {
        // ============
        // Given
        RecordHeader firstHeader = new RecordHeader();
        firstHeader.setRecordDataLength(123);
        firstHeader.setRecordDataCapacity(200);
        // ============
        // When
        RecordHeader nextHeader = firstHeader.nextHeader();
        // ============
        // Then
        assertThat(nextHeader.getStartPointer()).isEqualTo(165L);
        assertThat(nextHeader.getStartDataPointer()).isEqualTo(249L);
        assertThat(nextHeader.getEndPointer()).isEqualTo(249L);
        assertThat(nextHeader.getLength()).isEqualTo(84);
        assertThat(nextHeader.getRecordDataCapacity()).isEqualTo(0);
        assertThat(nextHeader.getRecordDataLength()).isEqualTo(0);
        assertThat(nextHeader.getRecordIndex()).isEqualTo(1);
    }

    @Test
    public void testReadWrite_ByteBuffer() {
        // ============
        // Given
        RecordHeader givenHeader = new RecordHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getLength());
        int dataBlockCapacity = 12;
        int occupiedBytes = 45;
        int startDataPointer = 80;
        int startPointer = 78;
        givenHeader.setRecordDataCapacity(dataBlockCapacity);
        givenHeader.setRecordDataLength(occupiedBytes);
        givenHeader.setStartDataPointer(startDataPointer);
        givenHeader.setStartPointer(startPointer);
        givenHeader.setDeleted(true);
        // ============
        // When
        RecordHeader whenHeader = new RecordHeader();
        givenHeader.write(buffer);
        buffer.flip();
        whenHeader.read(buffer);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(whenHeader.getRecordDataCapacity());
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(whenHeader.getRecordDataLength());
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(whenHeader.getStartDataPointer());
        assertThat(givenHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(whenHeader.getLastModifiedTimeMillis());
        assertThat(givenHeader.isDeleted()).isTrue();
        assertThat(givenHeader.isDeleted()).isEqualTo(whenHeader.isDeleted());

        assertThat(givenHeader.getStartPointer()).isEqualTo(startPointer);
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(dataBlockCapacity);
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(occupiedBytes);
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(startDataPointer);
    }

    @Test
    public void testReadWrite_File_Channel() throws IOException {
        // ============
        // Given
        RecordHeader givenHeader = new RecordHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getLength());
        int byteBufferCapacity = 5000;
        int dataBlockCapacity = 12;
        int occupiedBytes = 45;
        int startDataPointer = 85;
        int startPointer = 1230;
        givenHeader.setRecordDataCapacity(dataBlockCapacity);
        givenHeader.setRecordDataLength(occupiedBytes);
        givenHeader.setStartDataPointer(startDataPointer);
        givenHeader.setStartPointer(startPointer);

        ByteBuffer buffer1 = ByteBuffer.allocate(byteBufferCapacity);
        ByteBuffer buffer2 = ByteBuffer.allocate(byteBufferCapacity);

        file = new File("RecordHeaderTest.testReadWrite_Channel.bin");
        Path path = file.toPath();
        channel1 = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        channel2 = Files.newByteChannel(path, StandardOpenOption.READ);
        // ============
        // When
        RecordHeader whenHeader = new RecordHeader();
        givenHeader.write(channel1, buffer1);
        whenHeader.setStartPointer(startPointer);
        whenHeader.read(channel2, buffer2);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(whenHeader.getRecordDataCapacity());
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(whenHeader.getRecordDataLength());
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(whenHeader.getStartDataPointer());
        assertThat(givenHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(whenHeader.getLastModifiedTimeMillis());
        assertThat(givenHeader.isDeleted()).isFalse();
        assertThat(givenHeader.isDeleted()).isEqualTo(whenHeader.isDeleted());

        assertThat(givenHeader.getStartPointer()).isEqualTo(startPointer);
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(dataBlockCapacity);
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(occupiedBytes);
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(startDataPointer);
    }


    @Test
    public void testReadWrite_InMemory_Channel() throws IOException {
        // ============
        // Given
        RecordHeader givenHeader = new RecordHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getLength());
        int byteBufferCapacity = 5000;
        int dataBlockCapacity = 12;
        int occupiedBytes = 45;
        int startDataPointer = 85;
        int startPointer = 1230;
        givenHeader.setRecordDataCapacity(dataBlockCapacity);
        givenHeader.setRecordDataLength(occupiedBytes);
        givenHeader.setStartDataPointer(startDataPointer);
        givenHeader.setStartPointer(startPointer);
        givenHeader.setDeleted(true);

        ByteBuffer buffer1 = ByteBuffer.allocate(byteBufferCapacity);
        ByteBuffer buffer2 = ByteBuffer.allocate(byteBufferCapacity);

        byte[] contents = new byte[byteBufferCapacity];
        channel1 = new SeekableInMemoryByteChannel(contents);
        channel2 = new SeekableInMemoryByteChannel(contents);
        // ============
        // When
        RecordHeader whenHeader = new RecordHeader();
        givenHeader.write(channel1, buffer1);
        whenHeader.setStartPointer(startPointer);
        whenHeader.read(channel2, buffer2);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(whenHeader.getRecordDataCapacity());
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(whenHeader.getRecordDataLength());
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(whenHeader.getStartDataPointer());
        assertThat(givenHeader.getLastModifiedTimeMillis()).isLessThanOrEqualTo(whenHeader.getLastModifiedTimeMillis());
        assertThat(givenHeader.isDeleted()).isTrue();
        assertThat(givenHeader.isDeleted()).isEqualTo(whenHeader.isDeleted());

        assertThat(givenHeader.getStartPointer()).isEqualTo(startPointer);
        assertThat(givenHeader.getRecordDataCapacity()).isEqualTo(dataBlockCapacity);
        assertThat(givenHeader.getRecordDataLength()).isEqualTo(occupiedBytes);
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(startDataPointer);
    }

}
