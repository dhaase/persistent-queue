package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.data.RecordData;
import eu.dirk.haase.io.storage.record.header.MainHeader;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dhaa on 17.07.17.
 */
public class SharedTailPointer {

    private final AtomicLong tailPointerAtomic;
    private final int sumRecordHeaderLength;
    private final ReentrantLock lock;
    private long tailPointerPrimitiv;

    public SharedTailPointer() {
        MainHeader mainHeader = new MainHeader();
        RecordData recordData = new RecordData();
        RecordHeader recordHeader = new RecordHeader();

        this.lock = new ReentrantLock(true);
        this.tailPointerPrimitiv = mainHeader.getLength();
        this.tailPointerAtomic = new AtomicLong(mainHeader.getLength());
        this.sumRecordHeaderLength = recordData.getLength() + recordHeader.getLength();
    }

    public long nextLock(long dataLength) throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            long oldTailPointer = this.tailPointerPrimitiv;
            this.tailPointerPrimitiv += dataLength;
            return oldTailPointer;
        } finally {
            this.lock.unlock();
        }
    }

    public long nextSync(long dataLength) {
        synchronized (this) {
            long oldTailPointer = this.tailPointerPrimitiv;
            this.tailPointerPrimitiv += dataLength;
            return oldTailPointer;
        }
    }

    public long nextAtomic(long dataLength) {
        return this.tailPointerAtomic.getAndAdd(dataLength);
    }


    public int calcRecordLength(ByteBuffer dataBuffer) {
        return (dataBuffer != null ? dataBuffer.position() : 0) + this.sumRecordHeaderLength;
    }


    public void clear() {
        synchronized (this) {
            this.tailPointerPrimitiv = 0;
            this.tailPointerAtomic.set(0);
        }
    }

}
