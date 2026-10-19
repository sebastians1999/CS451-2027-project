package cs451;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

// The report is printed from close. Printing each ack would inflate the next RTTs.
final class RttSampleLog {
    private static final class Pending {
        final int dest;
        final int seq;
        final long sendNs;
        // Retransmit thread sets this. The receive thread reads it when the ack removes the sample.
        volatile boolean retransmitted;

        Pending(int dest, int seq, long sendNs) {
            this.dest = dest;
            this.seq = seq;
            this.sendNs = sendNs;
        }
    }

    private static final class Row {
        final long rttNs;
        final boolean retransmitted;

        Row(long rttNs, boolean retransmitted) {
            this.rttNs = rttNs;
            this.retransmitted = retransmitted;
        }
    }

    private static final int[] WINDOWS = {1, 8, 32, 128, 1024};

    private final int myId;
    private final int window;
    private final long timeoutMs;
    private final int skip;
    private final int max;
    private final int senders;
    private final Path donePath;
    private final ConcurrentHashMap<Long, Pending> inflight = new ConcurrentHashMap<>();
    private final List<Row> completed = new ArrayList<>();

    private int seen;
    private int extraSends;
    private boolean markerWritten;
    // Read by the retransmit thread outside this object's lock.
    private volatile boolean closed;

    private RttSampleLog(int myId, int window, long timeoutMs, int skip, int max, int senders, Path donePath) {
        this.myId = myId;
        this.window = window;
        this.timeoutMs = timeoutMs;
        this.skip = skip;
        this.max = max;
        this.senders = senders;
        this.donePath = donePath;
    }

    static RttSampleLog open(int myId, int window, long timeoutMs) {
        String raw = System.getenv("RTT_LOG");
        if (raw == null || raw.isBlank()) {
            return null;
        }
        int skip = envInt("RTT_SKIP", 1000);
        int max = envInt("RTT_MAX", 20000);
        if (skip < 0) {
            throw new IllegalArgumentException("RTT_SKIP must be >= 0, got " + skip);
        }
        if (max < 1) {
            throw new IllegalArgumentException("RTT_MAX must be >= 1, got " + max);
        }
        int senders = envInt("RTT_SENDERS", 1);
        if (senders < 1) {
            throw new IllegalArgumentException("RTT_SENDERS must be >= 1, got " + senders);
        }
        return new RttSampleLog(myId, window, timeoutMs, skip, max, senders, donePath(Paths.get(raw.trim()), myId));
    }

    synchronized void onFirstSend(int dest, int seq, long sendNs) {
        if (closed) {
            return;
        }
        seen++;
        if (seen <= skip) {
            return;
        }
        if ((long) seen > (long) skip + max) {
            extraSends++;
            return;
        }
        inflight.put(key(dest, seq), new Pending(dest, seq, sendNs));
    }

    void onRetransmit(int dest, int seq) {
        if (closed) {
            return;
        }
        Pending pending = inflight.get(key(dest, seq));
        if (pending == null) {
            return;
        }
        pending.retransmitted = true;
    }

    synchronized void onAck(int src, int seq, long ackNs) {
        if (closed) {
            return;
        }
        Pending pending = inflight.remove(key(src, seq));
        if (pending == null) {
            return;
        }
        completed.add(new Row(ackNs - pending.sendNs, pending.retransmitted));
        if (!markerWritten && (long) seen >= (long) skip + max && inflight.isEmpty()) {
            markerWritten = true;
            // The graded output buffer does not flush until shutdown, so the runner cannot watch it. The marker means every recorded sample has been acked.
            try {
                Files.write(donePath, new byte[0]);
            } catch (IOException e) {
                System.err.println("Writing RTT done marker failed: " + e);
            }
        }
    }

    // Same lock as onAck, so a row is either in this write or it arrives after close and is dropped.
    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (completed.isEmpty() && seen == 0) {
            return;
        }
        System.out.print(report());
    }

    private String report() {
        int retransmitted = 0;
        for (Row row : completed) {
            if (row.retransmitted) {
                retransmitted++;
            }
        }
        StringBuilder out = new StringBuilder();
        out.append("--- RTT sample id=").append(myId)
                .append(" window=").append(window)
                .append(" timeout_ms=").append(timeoutMs)
                .append(" skip=").append(skip)
                .append(" max=").append(max)
                .append(" ---\n");
        out.append("recorded=").append(completed.size())
                .append(" extra_sends=").append(extraSends)
                .append(" retransmitted=").append(retransmitted)
                .append('\n');
        if (completed.isEmpty()) {
            out.append("--- end RTT sample ---\n");
            return out.toString();
        }
        if (retransmitted * 100 > completed.size()) {
            out.append("The sample timeout was too short. The dropped rows are the slow ones, so the percentiles are biased low.\n");
        }
        if (extraSends > 0) {
            out.append("The sender continued after RTT_MAX, so this report misses later peers.\n");
        }
        if (window > 1) {
            out.append("This window held more than one message, so the spread includes queueing.\n");
        }
        long[] clean = new long[completed.size() - retransmitted];
        int n = 0;
        for (Row row : completed) {
            if (!row.retransmitted) {
                clean[n++] = row.rttNs;
            }
        }
        if (n == 0) {
            out.append("Every completed sample was retransmitted. Raise TIMEOUT_MS and run again.\n");
            out.append("--- end RTT sample ---\n");
            return out.toString();
        }
        Arrays.sort(clean);
        out.append(String.format(Locale.US, "%-6s %10s%n", "", "us"));
        String[] names = {"min", "p50", "p90", "p95", "p99", "p99.9", "max"};
        double[] qs = {0, 50, 90, 95, 99, 99.9, 100};
        double p99 = 0;
        for (int i = 0; i < names.length; i++) {
            double ns = qs[i] == 0 ? clean[0] : qs[i] == 100 ? clean[n - 1] : percentile(clean, n, qs[i]);
            if (names[i].equals("p99")) {
                p99 = ns;
            }
            out.append(String.format(Locale.US, "%-6s %10.1f%n", names[i], ns / 1e3));
        }
        out.append("timeout_ms = max(1, ceil(2 * S * W * p99_us / 1000))\n");
        out.append("S=").append(senders)
                .append(" is the number of processes sending to the same receiver. For a real run, S is n - 1.\n");
        out.append(String.format(Locale.US, "%8s %12s%n", "window", "timeout_ms"));
        for (int candidate : WINDOWS) {
            out.append(String.format(Locale.US, "%8d %12d%n", candidate, timeoutMillis(senders, candidate, p99)));
        }
        out.append("largest window a 100 ms timeout can drain: ")
                .append(maxWindow(senders, p99))
                .append('\n');
        out.append("--- end RTT sample ---\n");
        return out.toString();
    }

    private static double percentile(long[] sorted, int n, double q) {
        if (n == 1) {
            return sorted[0];
        }
        double k = (n - 1) * q / 100.0;
        int lo = (int) Math.floor(k);
        int hi = Math.min(lo + 1, n - 1);
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo);
    }

    private static int timeoutMillis(int senders, int window, double p99Ns) {
        return Math.max(1, (int) Math.ceil(2.0 * senders * window * p99Ns / 1e6));
    }

    private static int maxWindow(int senders, double p99Ns) {
        double p99Ms = p99Ns / 1e6;
        if (p99Ms <= 0) {
            return 1;
        }
        return Math.max(1, (int) Math.floor(100.0 / (2.0 * senders * p99Ms)));
    }

    private static int envInt(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null) {
            return fallback;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " is blank");
        }
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got " + raw, e);
        }
    }

    private static Path donePath(Path given, int myId) {
        try {
            ensureDirectory(given);
            return given.resolve("rtt-" + myId + ".done");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // createDirectories throws when the path is a symlink to a directory, which /tmp is on macOS.
    private static void ensureDirectory(Path dir) throws IOException {
        if (dir == null || Files.isDirectory(dir)) {
            return;
        }
        Files.createDirectories(dir);
    }

    private static long key(int dest, int seq) {
        return ((long) dest << 32) | (seq & 0xFFFFFFFFL);
    }
}
