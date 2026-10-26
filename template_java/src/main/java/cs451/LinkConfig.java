package cs451;

final class LinkConfig {
    final int window;       // max unACKed messages in flight per destination
    final int timeoutMs;   // retransmit interval for stubbon links

    LinkConfig(int window, int timeoutMs) {
        if (window < 1 || timeoutMs < 1) {
            throw new IllegalArgumentException("window and timeoutMs must be >= 1, got " + window + ", " + timeoutMs);
        }
        this.window = window;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public String toString() {
        return "window=" + window + " timeoutMs=" + timeoutMs;
    }
}
