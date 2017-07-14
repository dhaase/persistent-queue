package hamner.db;

import java.io.*;

public class RecordReader {

    byte[] data;
    String key;
    ByteArrayInputStream in;
    ObjectInputStream objIn;

    public RecordReader(String key, byte[] data) {
        this.key = key;
        this.data = data;
        in = new ByteArrayInputStream(data);
    }

    public byte[] getData() {
        return data;
    }

    public String getKey() {
        return key;
    }

    public InputStream getInputStream() throws IOException {
        return in;
    }

    public ObjectInputStream getObjectInputStream() throws IOException {
        if (objIn == null) {
            objIn = new ObjectInputStream(in);
        }
        return objIn;
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        return getObjectInputStream().readObject();
    }
}






