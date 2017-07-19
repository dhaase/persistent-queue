package eu.dirk.haase.io.storage.record;

import eu.dirk.haase.io.storage.record.data.RecordData;
import eu.dirk.haase.io.storage.record.header.MainHeader;
import eu.dirk.haase.io.storage.record.header.RecordHeader;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dhaa on 17.07.17.
 */
public class Shared {

    private final AtomicLong atomicTailPointer;
    private final int sumRecordHeaderLength;
    private long tailPointer;

    public Shared() {
        tailPointer = 0;
        MainHeader mainHeader = new MainHeader();
        RecordData recordData = new RecordData();
        RecordHeader recordHeader = new RecordHeader();

        this.atomicTailPointer = new AtomicLong(mainHeader.getLength());
        this.sumRecordHeaderLength = recordData.getLength() + recordHeader.getLength();
    }

    public long next(int dataLength) {
        synchronized (this) {
            long oldTailPointer = tailPointer;
            tailPointer = (tailPointer + dataLength);
            return oldTailPointer;
        }
        //return this.atomicTailPointer.getAndAdd(dataLength);
    }


    public int calcRecordLength(ByteBuffer dataBuffer) {
        return (dataBuffer != null ? dataBuffer.position() : 0) + this.sumRecordHeaderLength;
    }

}
