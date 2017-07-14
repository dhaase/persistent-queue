package hamner.db;

import java.io.*;

public class RecordWriter {

    String key;
    ByteArrayOutputStream out;
    ObjectOutputStream objOut;

    public RecordWriter(String key) {
        this.key = key;
        out = new ByteArrayOutputStream();
    }

    public String getKey() {
        return key;
    }

    public OutputStream getOutputStream() {
        return out;
    }

    public ObjectOutputStream getObjectOutputStream() throws IOException {
        if (objOut == null) {
            objOut = new ObjectOutputStream(out);
        }
        return objOut;
    }

    public void writeObject(Object o) throws IOException {
        getObjectOutputStream().writeObject(o);
        getObjectOutputStream().flush();
    }

    /**
     * Returns the number of bytes in the data.
     */
    public int getDataLength() {
        return out.size();
    }

    /**
     * Writes the data out to the stream without re-allocating the buffer.
     */
    public void writeTo(DataOutput dstr) throws IOException {
        byte[] data = out.toByteArray();
        int l = out.size();
        dstr.write(data, 0, l);
    }

    /**
     * Writes the data out to the stream without re-allocating the buffer.
     */
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
    }

}






