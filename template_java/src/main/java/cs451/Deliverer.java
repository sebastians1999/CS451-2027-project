package cs451;

public interface Deliverer {
    // false means refused: the link must not ack, so the sender retransmits.
    // relayer is who sent this datagram; origin and seq belong to the original sender.
    boolean deliver(int relayer, int origin, int seq, int payload);
}
