package cs451.perfectlinks;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import cs451.Host;

final class StubbornLink implements PacketHandler {
    private final long timeoutMs;
    private final PerfectLink upper;
    private final ConcurrentHashMap<Integer, Message>[] pending; // keyed by link seq, so a relay cannot collide with our own seq
    private final List<Message>[] batch;             // no lock: only the main thread calls send and flush
    private final Semaphore[] window;                //array otherwise a dead process (receiver) could take up all the slots, Semaphore for threadsafety
    private final Transport transport;
    private final Thread retransmitter;
    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    StubbornLink(int myId, List<Host> hosts, LinkConfig config, PerfectLink upper) throws SocketException {
        int numHosts = hosts.size();
        this.timeoutMs = config.timeoutMs;
        this.upper = upper;
        this.pending = new ConcurrentHashMap[numHosts + 1];
        this.batch = new ArrayList[numHosts + 1];
        this.window = new Semaphore[numHosts + 1];

        for (int id = 1; id <= numHosts; id++) {
            pending[id] = new ConcurrentHashMap<>();
            batch[id] = new ArrayList<>(Transport.MAX_BATCH);
            window[id] = new Semaphore(config.window);   //for memory safety
        }

        this.transport = new UdpTransport(myId, hosts, this);

        this.retransmitter = new Thread(this::retransmitLoop, "retransmit");
        retransmitter.start();
        // Last: once receiving starts, onData/onAck can run and need every field above.
        transport.start();
    }

    void send(int dest, int linkSeq, int origin, int seq, int payload) throws InterruptedException {
        if (!window[dest].tryAcquire()) {
            flush(dest);
            window[dest].acquire();
        }
        // Must be in pending before it hits the wire, or an early ACK finds nothing to remove.
        Message message = new Message(linkSeq, origin, seq, payload);
        pending[dest].put(linkSeq, message);
        batch[dest].add(message);
        if (batch[dest].size() == Transport.MAX_BATCH) {
            flush(dest);
        }
    }

    void flush(int dest) {
        List<Message> buf = batch[dest];
        if (buf.isEmpty()) {
            return;
        }
        transport.sendData(dest, buf);
        buf.clear();
    }

    @Override
    public void onData(int src, List<Message> received) {
        int[] linkSeqs = new int[received.size()];
        int accepted = 0;
        for (int i = 0; i < received.size(); i++) {
            Message message = received.get(i);
            if (upper.deliver(src, message.linkSeq, message.origin, message.seq, message.payload)) {
                linkSeqs[accepted++] = message.linkSeq;
            }
        }
        // No ACK at all when every message was refused: the sender must keep retransmitting.
        if (accepted == 0) {
            return;
        }
        if (accepted < linkSeqs.length) {
            int[] exact = new int[accepted];
            System.arraycopy(linkSeqs, 0, exact, 0, accepted);
            linkSeqs = exact;
        }
        transport.sendAck(src, linkSeqs);
    }

    @Override
    public void onAck(int src, int[] linkSeqs) {
        for (int linkSeq : linkSeqs) {
            if (pending[src].remove(linkSeq) != null) { // removes it AND checks it was there, if already have been removed it will return null
                window[src].release();
            }
        }
    }

    private void retransmitLoop() {
        List<Message> chunk = new ArrayList<>(Transport.MAX_BATCH);
        while (running) {
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                return;
            }
            for (int dest = 1; dest < pending.length; dest++) {
                for (Message message : pending[dest].values()) {
                    chunk.add(message);
                    if (chunk.size() == Transport.MAX_BATCH) {
                        transport.sendData(dest, chunk);
                        chunk.clear();
                    }
                }
                if (!chunk.isEmpty()) {
                    transport.sendData(dest, chunk);
                    chunk.clear();
                }
            }
        }
    }

    void stop() {
        running = false;
        transport.close();
        retransmitter.interrupt();
    }
}
