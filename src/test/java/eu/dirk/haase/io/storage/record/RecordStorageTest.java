package eu.dirk.haase.io.storage.record;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * Created by dhaa on 15.07.17.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RecordStorageTest {

    protected RecordStorage recordStorage;
    protected SeekableByteChannel channel;
    private File file;

    @Before
    public void setUp() throws IOException {
        OpenOption[] openOption = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ};
        file = new File("./RecordChannelStorageFileTest.recordfile.bin");
        recordStorage = new StribedRecordStorage(10, file, openOption);
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
    public void testSelectRecord_WithTwoRecords() throws IOException, InterruptedException {
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

        recordStorage.create();

        Thread t1 = new Thread(new Task(recordStorage));
        Thread t2 = new Thread(new Task(recordStorage));
        Thread t3 = new Thread(new Task(recordStorage));
        Thread t4 = new Thread(new Task(recordStorage));

        t1.setDaemon(true);
        t2.setDaemon(true);
        t3.setDaemon(true);
        t4.setDaemon(true);

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        t1.join();
        t2.join();
        t3.join();
        t4.join();
    }


    class Task implements Runnable {
        int CAPACITY = 1024;
        byte[] buffer1b = new byte[CAPACITY];
        byte[] buffer2b = new byte[CAPACITY];

        RecordStorage recordStorage;
        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        ByteBuffer dataByteBuffer1 = ByteBuffer.allocate(CAPACITY);
        ByteBuffer dataByteBuffer2 = ByteBuffer.allocate(CAPACITY);
        ByteBuffer dataByteBuffer1b = ByteBuffer.wrap(buffer1b);
        ByteBuffer dataByteBuffer2b = ByteBuffer.wrap(buffer2b);

        Task(RecordStorage recordStorage) {
            this.recordStorage = recordStorage;
        }

        @Override
        public void run() {
            System.out.println("start");
            for (int i = 0; 1000 > i; ++i) {
                try {
                    recordStorage.insertRecord(key1, dataByteBuffer1);
                    recordStorage.insertRecord(key2, dataByteBuffer2);
                    recordStorage.selectRecord(key1, dataByteBuffer1b);
                    recordStorage.selectRecord(key2, dataByteBuffer2b);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("end");
        }
    }

}