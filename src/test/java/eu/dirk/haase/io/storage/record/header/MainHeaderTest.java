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
public class MainHeaderTest {

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
        MainHeader givenHeader = new MainHeader();
        ByteBuffer buffer = ByteBuffer.allocate(givenHeader.getLength());
        int dataBlockCount = 12;
        int minDataBlockLength = 23;
        int maxDataBlockLength = 45;
        givenHeader.setRecordCount(dataBlockCount);
        givenHeader.setMinRecordDataLength(minDataBlockLength);
        givenHeader.setMaxRecordDataLength(maxDataBlockLength);
        // ============
        // When
        givenHeader.write(buffer);
        MainHeader whenHeader = new MainHeader();
        buffer.flip();
        whenHeader.read(buffer);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(0);
        assertThat(givenHeader.getVersion()).isEqualTo(1);
        assertThat(givenHeader.isCompabible()).isTrue();

        assertThat(givenHeader.getVersion()).isEqualTo(whenHeader.getVersion());
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getRecordCount()).isEqualTo(whenHeader.getRecordCount());
        assertThat(givenHeader.getMinRecordDataLength()).isEqualTo(whenHeader.getMinRecordDataLength());
        assertThat(givenHeader.getMaxRecordDataLength()).isEqualTo(whenHeader.getMaxRecordDataLength());

        assertThat(givenHeader.getRecordCount()).isEqualTo(dataBlockCount);
        assertThat(givenHeader.getMinRecordDataLength()).isEqualTo(minDataBlockLength);
        assertThat(givenHeader.getMaxRecordDataLength()).isEqualTo(maxDataBlockLength);
    }


    @Test
    public void testReadWrite_File_Channel() throws IOException {
        // ============
        // Given
        MainHeader givenHeader = new MainHeader();
        int byteBufferCapacity = 5000;
        int dataBlockCount = 12;
        int minDataBlockLength = 23;
        int maxDataBlockLength = 45;
        givenHeader.setRecordCount(dataBlockCount);
        givenHeader.setMinRecordDataLength(minDataBlockLength);
        givenHeader.setMaxRecordDataLength(maxDataBlockLength);

        ByteBuffer buffer1 = ByteBuffer.allocate(byteBufferCapacity);
        ByteBuffer buffer2 = ByteBuffer.allocate(byteBufferCapacity);

        file = new File("MainHeaderTest.testReadWrite_Channel.bin");
        Path path = file.toPath();
        channel1 = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        channel2 = Files.newByteChannel(path, StandardOpenOption.READ);
        // ============
        // When
        MainHeader whenHeader = new MainHeader();
        givenHeader.write(channel1, buffer1);
        whenHeader.read(channel2, buffer2);
        // ============
        // Then
        assertThat(givenHeader.getStartPointer()).isEqualTo(0);
        assertThat(givenHeader.getVersion()).isEqualTo(1);
        assertThat(givenHeader.isCompabible()).isTrue();

        assertThat(givenHeader.getVersion()).isEqualTo(whenHeader.getVersion());
        assertThat(givenHeader.getStartPointer()).isEqualTo(whenHeader.getStartPointer());
        assertThat(givenHeader.getRecordCount()).isEqualTo(whenHeader.getRecordCount());
        assertThat(givenHeader.getMinRecordDataLength()).isEqualTo(whenHeader.getMinRecordDataLength());
        assertThat(givenHeader.getMaxRecordDataLength()).isEqualTo(whenHeader.getMaxRecordDataLength());

        assertThat(givenHeader.getRecordCount()).isEqualTo(dataBlockCount);
        assertThat(givenHeader.getMinRecordDataLength()).isEqualTo(minDataBlockLength);
        assertThat(givenHeader.getMaxRecordDataLength()).isEqualTo(maxDataBlockLength);
    }

}
