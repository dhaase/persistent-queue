package eu.dirk.haase.io.storage.record.header;

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
public class StorageHeaderTest {

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
        StorageHeader givenHeader = new StorageHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getHeaderLength());
        int dataBlockCount = 12;
        int minDataBlockLength = 23;
        int maxDataBlockLength = 45;
        givenHeader.setDataBlockCount(dataBlockCount);
        givenHeader.setMinDataBlockLength(minDataBlockLength);
        givenHeader.setMaxDataBlockLength(maxDataBlockLength);
        // ============
        // When
        givenHeader.write(buffer);
        StorageHeader whenHeader = new StorageHeader();
        buffer.flip();
        whenHeader.read(buffer);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(0);
        assertThat(givenHeader.getVersion()).isEqualTo(1);
        assertThat(givenHeader.isCompabible()).isTrue();

        assertThat(givenHeader.getVersion()).isEqualTo(whenHeader.getVersion());
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getDataBlockCount()).isEqualTo(whenHeader.getDataBlockCount());
        assertThat(givenHeader.getMinDataBlockLength()).isEqualTo(whenHeader.getMinDataBlockLength());
        assertThat(givenHeader.getMaxDataBlockLength()).isEqualTo(whenHeader.getMaxDataBlockLength());

        assertThat(givenHeader.getDataBlockCount()).isEqualTo(dataBlockCount);
        assertThat(givenHeader.getMinDataBlockLength()).isEqualTo(minDataBlockLength);
        assertThat(givenHeader.getMaxDataBlockLength()).isEqualTo(maxDataBlockLength);
    }


    @Test
    public void testReadWrite_Channel() throws IOException {
        // ============
        // Given
        StorageHeader givenHeader = new StorageHeader();
        int dataBlockCount = 12;
        int minDataBlockLength = 23;
        int maxDataBlockLength = 45;
        givenHeader.setDataBlockCount(dataBlockCount);
        givenHeader.setMinDataBlockLength(minDataBlockLength);
        givenHeader.setMaxDataBlockLength(maxDataBlockLength);

        ByteBuffer buffer1 = ByteBuffer.allocate(givenHeader.getHeaderLength());
        ByteBuffer buffer2 = ByteBuffer.allocate(givenHeader.getHeaderLength());

        file = new File("StorageHeaderTest.testReadWrite_Channel.bin");
        Path path = file.toPath();
        channel1 = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        channel2 = Files.newByteChannel(path, StandardOpenOption.READ);
        // ============
        // When
        StorageHeader whenHeader = new StorageHeader();
        givenHeader.write(channel1, buffer1);
        whenHeader.read(channel2, buffer2);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(0);
        assertThat(givenHeader.getVersion()).isEqualTo(1);
        assertThat(givenHeader.isCompabible()).isTrue();

        assertThat(givenHeader.getVersion()).isEqualTo(whenHeader.getVersion());
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getDataBlockCount()).isEqualTo(whenHeader.getDataBlockCount());
        assertThat(givenHeader.getMinDataBlockLength()).isEqualTo(whenHeader.getMinDataBlockLength());
        assertThat(givenHeader.getMaxDataBlockLength()).isEqualTo(whenHeader.getMaxDataBlockLength());

        assertThat(givenHeader.getDataBlockCount()).isEqualTo(dataBlockCount);
        assertThat(givenHeader.getMinDataBlockLength()).isEqualTo(minDataBlockLength);
        assertThat(givenHeader.getMaxDataBlockLength()).isEqualTo(maxDataBlockLength);
    }

}
