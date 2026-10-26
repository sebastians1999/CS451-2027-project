package cs451;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

// One per process. All methods are synchronized so a line is always appended whole,
// and the shutdown hook's close() can never write half a line or interleave with an append.
final class OutputLog {
    private static final int FLUSH_THRESHOLD = 1_000_000;   // write to the file once this many chars are buffered

    private final Writer writer;
    private final StringBuilder buffer = new StringBuilder();
    private boolean closed = false;

    OutputLog(String path) throws IOException {
        this.writer = new FileWriter(path);
    }

    // Call before the message goes on the wire, so a crash never leaves a "d" without its "b".
    synchronized void sent(int seq) {
        if (closed) {
            return;
        }
        buffer.append("b ").append(seq).append('\n');
        flushIfFull();
    }

    synchronized void delivered(int src, int seq) {
        if (closed) {
            return;
        }
        buffer.append("d ").append(src).append(' ').append(seq).append('\n');
        flushIfFull();
    }

    // After this, sent() and delivered() are ignored: events after shutdown must not be logged.
    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writeBuffer();
            writer.close();
        } catch (IOException e) {
            System.err.println("Writing output failed: " + e);
        }
    }

    private void flushIfFull() {
        if (buffer.length() < FLUSH_THRESHOLD) {
            return;
        }
        try {
            writeBuffer();
        } catch (IOException e) {
            System.err.println("Writing output failed: " + e);
        }
    }

    private void writeBuffer() throws IOException {
        writer.write(buffer.toString());
        writer.flush();
        buffer.setLength(0);
    }
}
