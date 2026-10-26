package cs451;

interface Transport {
    void sendData(int dest, int seq, int payload);

    void sendAck(int dest, int seq);

    void start();

    void close();
}
