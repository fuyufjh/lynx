package me.ericfu.lightning.source.text;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.data.ByteString;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * TextValueReader reads a single value from current InputStream
 */
final class TextValueReader {

    private enum EndWith {
        NEW_LINE, SEPARATOR, EOF
    }

    private final BufferedInputStream in;
    private final ByteArrayOutputStream out;
    private final int sep;

    private int line;
    private EndWith end;

    public TextValueReader(BufferedInputStream in, byte separator) {
        this.in = in;
        this.out = new ByteArrayOutputStream();
        this.sep = separator;
        this.line = 1;
    }

    /**
     * Read next value in ByteString
     *
     * @return next value or null for EOF
     */
    public ByteString readString() throws IOException {
        for (; ; ) {
            final int b = in.read();
            if (b < 0) {
                end = EndWith.EOF;
                return null;
            } else if (isNewLine(b)) {
                end = EndWith.NEW_LINE;
                break;
            } else if (b == sep) {
                end = EndWith.SEPARATOR;
                break;
            } else if (b == '"') {
                if (out.size() == 0) {
                    return readQuotedString();
                } else {
                    throw new IOException("unexpected quote mark at line " + line);
                }
            } else {
                out.write(b);
            }
        }
        return new ByteString(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private ByteString readQuotedString() throws IOException {
        int b;
        for (; ; ) {
            b = in.read();
            if (b < 0) {
                throw new EOFException("unclosed string at line " + line);
            } else if (b == '"') {
                b = in.read();
                if (b == '"') {
                    out.write('"');
                } else {
                    break;
                }
            } else {
                out.write(b);
            }
        }

        if (b == sep) {
            end = EndWith.SEPARATOR;
        } else if (isNewLine(b)) {
            end = EndWith.NEW_LINE;
        } else {
            throw new IOException("expect new-line or separator but got '" + ((char) b) + "' at line " + line);
        }

        return new ByteString(out.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Check new-line character (CR, LF or CRLF) and consume it
     */
    private boolean isNewLine(int b) throws IOException {
        if (b == '\n' || b == '\r') {
            if (b == '\r') {
                // Maybe it is a CRLF?
                in.mark(1);
                int b2 = in.read();
                if (b2 != '\n') {
                    in.reset();
                }
            }
            line++;
            return true;
        }
        return false;
    }

    public void reset() {
        end = null;
        out.reset();
    }

    public boolean isEndWithNewLine() {
        Preconditions.checkState(end != null);
        return end == EndWith.NEW_LINE;
    }

    public boolean isEndWithSeparator() {
        Preconditions.checkState(end != null);
        return end == EndWith.SEPARATOR;
    }

    public int getLineNumber() {
        return line;
    }
}
