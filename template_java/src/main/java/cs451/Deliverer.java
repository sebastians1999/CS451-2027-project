package cs451;

public interface Deliverer {
    // false means refused: the link must not ack, so the sender retransmits.
    boolean deliver(int src, int seq, int payload);
}
