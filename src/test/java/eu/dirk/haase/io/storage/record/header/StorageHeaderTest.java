package eu.dirk.haase.io.storage.record.header;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dhaa on 14.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class StorageHeaderTest {

    @Test
    public void testReadWrite() {
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


}
