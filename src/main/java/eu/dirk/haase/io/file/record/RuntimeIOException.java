package eu.dirk.haase.io.file.record;

/**
 * Created by dhaa on 14.07.17.
 */
public class RuntimeIOException extends RuntimeException {

    public RuntimeIOException() {
    }

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuntimeIOException(Throwable cause) {
        super(cause);
    }
}
