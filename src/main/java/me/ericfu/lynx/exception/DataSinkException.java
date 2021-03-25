package me.ericfu.lynx.exception;

public class DataSinkException extends Exception {

    public DataSinkException(String message) {
        super(message);
    }

    public DataSinkException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSinkException(Throwable cause) {
        super(cause);
    }
}
