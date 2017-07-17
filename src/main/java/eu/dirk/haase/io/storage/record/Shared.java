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

    private final AtomicLong tailPointer;
    private final int sumRecordHeaderLength;

    public Shared() {
        MainHeader mainHeader = new MainHeader();
        RecordData recordData = new RecordData();
        RecordHeader recordHeader = new RecordHeader();

        this.tailPointer = new AtomicLong(mainHeader.getLength());
        this.sumRecordHeaderLength = recordData.getLength() + recordHeader.getLength();
    }

    public long getTailPointer() {
        return tailPointer.get();
    }

    public int next(ByteBuffer dataBuffer) {
        int dataLength = calcRecordLength(dataBuffer);
        this.tailPointer.addAndGet(dataLength);
        return dataLength;
    }


    private int calcRecordLength(ByteBuffer dataBuffer) {
        return (dataBuffer != null ? dataBuffer.position() : 0) + this.sumRecordHeaderLength;
    }

}
