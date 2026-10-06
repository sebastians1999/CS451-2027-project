package cs451;

interface PacketHandler {
    void onData(int src, int seq, int payload);

    void onAck(int src, int seq);
}
