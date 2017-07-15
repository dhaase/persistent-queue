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
public class DataBlockHeaderTest {

    @Test
    public void testReadWrite() {
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


}
