package eu.dirk.haase.io.storage.record.header;

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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 14.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class DataBlockHeaderTest {

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
    public void testReadWrite_ByteBuffer() {
        // ============
        // Given
        DataBlockHeader givenHeader = new DataBlockHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getHeaderLength());
        int dataBlockCapacity = 12;
        int dataBlockOccupied = 45;
        int startDataPointer = 67;
        int startPointer = 78;
        givenHeader.setDataBlockCapacity(dataBlockCapacity);
        givenHeader.setDataBlockOccupied(dataBlockOccupied);
        givenHeader.setStartDataPointer(startDataPointer);
        givenHeader.setStartPointer(startPointer);
        // ============
        // When
        DataBlockHeader whenHeader = new DataBlockHeader();
        givenHeader.write(buffer);
        buffer.flip();
        whenHeader.read(buffer);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getDataBlockCapacity()).isEqualTo(whenHeader.getDataBlockCapacity());
        assertThat(givenHeader.getDataBlockOccupied()).isEqualTo(whenHeader.getDataBlockOccupied());
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(whenHeader.getStartDataPointer());

        assertThat(givenHeader.getStartPointer()).isEqualTo(startPointer);
        assertThat(givenHeader.getDataBlockCapacity()).isEqualTo(dataBlockCapacity);
        assertThat(givenHeader.getDataBlockOccupied()).isEqualTo(dataBlockOccupied);
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(startDataPointer);
    }

    @Test
    public void testReadWrite_Channel() throws IOException {
        // ============
        // Given
        DataBlockHeader givenHeader = new DataBlockHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getHeaderLength());
        int dataBlockCapacity = 12;
        int dataBlockOccupied = 45;
        int startDataPointer = 67;
        int startPointer = 1230;
        givenHeader.setDataBlockCapacity(dataBlockCapacity);
        givenHeader.setDataBlockOccupied(dataBlockOccupied);
        givenHeader.setStartDataPointer(startDataPointer);
        givenHeader.setStartPointer(startPointer);

        ByteBuffer buffer0 = ByteBuffer.allocate(givenHeader.getHeaderLength());
        buffer0.putInt(1);
        ByteBuffer buffer1 = ByteBuffer.allocate(givenHeader.getHeaderLength());
        ByteBuffer buffer2 = ByteBuffer.allocate(givenHeader.getHeaderLength());

        file = new File("DataBlockHeaderTest.testReadWrite_Channel.bin");
        Path path = file.toPath();
        channel1 = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
//        channel1.position(startPointer + 5000);
//        buffer0.flip();
//        channel1.write(buffer0);

        channel2 = Files.newByteChannel(path, StandardOpenOption.READ);
        // ============
        // When
        DataBlockHeader whenHeader = new DataBlockHeader();
        givenHeader.write(channel1, buffer1);
        whenHeader.setStartPointer(startPointer);
        whenHeader.read(channel2, buffer2);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getDataBlockCapacity()).isEqualTo(whenHeader.getDataBlockCapacity());
        assertThat(givenHeader.getDataBlockOccupied()).isEqualTo(whenHeader.getDataBlockOccupied());
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(whenHeader.getStartDataPointer());

        assertThat(givenHeader.getStartPointer()).isEqualTo(startPointer);
        assertThat(givenHeader.getDataBlockCapacity()).isEqualTo(dataBlockCapacity);
        assertThat(givenHeader.getDataBlockOccupied()).isEqualTo(dataBlockOccupied);
        assertThat(givenHeader.getStartDataPointer()).isEqualTo(startDataPointer);
    }

}
