package me.ericfu.lynx.exception;

public class CheckpointException extends Exception {

    public CheckpointException(Throwable cause) {
        super(cause);
    }

    public CheckpointException(String message) {
        super(message);
    }
}
