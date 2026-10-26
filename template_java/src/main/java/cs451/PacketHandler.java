package cs451;

import java.util.List;

interface PacketHandler {
    void onData(int src, List<Message> batch);

    void onAck(int src, int[] seqs);
}
