package cs451;

import java.util.List;

interface Transport {
    int MAX_BATCH = 8;

    void sendData(int dest, List<Message> batch);

    void sendAck(int dest, int[] seqs);

    void start();

    void close();
}
