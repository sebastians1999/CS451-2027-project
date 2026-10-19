package cs451;

import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

final class StubbornLink implements PacketHandler {
    private final long timeoutMs;
    private final Deliverer upper;
    private final ConcurrentHashMap<Integer, Message>[] pending;
    private final Semaphore[] window;                //array otherwise a dead process (receiver) could take up all the slots, Semaphore for threadsafety
    private final Transport transport;
    private final RttSampleLog rtt;
    private final Thread retransmitter;
    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    StubbornLink(int myId, List<Host> hosts, LinkConfig config, Deliverer upper) throws SocketException {
        int numHosts = hosts.size();
        this.timeoutMs = config.timeoutMs;
        this.upper = upper;
        this.pending = new ConcurrentHashMap[numHosts + 1];
        this.window = new Semaphore[numHosts + 1];

        for (int id = 1; id <= numHosts; id++) {
            pending[id] = new ConcurrentHashMap<>();
            window[id] = new Semaphore(config.window);   //for memory safety
        }

        this.transport = new UdpTransport(myId, hosts, this);
        this.rtt = RttSampleLog.open(myId, config.window, config.timeoutMs);

        this.retransmitter = new Thread(this::retransmitLoop, "retransmit");
        retransmitter.start();
        // Last: once receiving starts, onData/onAck can run and need every field above.
        transport.start();
    }

    void send(int dest, int seq, int payload) throws InterruptedException {
        window[dest].acquire();
        // Must be in pending before it hits the wire, or an early ACK finds nothing to remove.
        pending[dest].put(seq, new Message(seq, payload));
        if (rtt != null) {
            rtt.onFirstSend(dest, seq, System.nanoTime());
        }
        transport.sendData(dest, seq, payload);
    }

    @Override
    public void onData(int src, int seq, int payload) {
        upper.deliver(src, seq, payload);
        transport.sendAck(src, seq);
    }

    @Override
    public void onAck(int src, int seq) {
        if (rtt != null) {
            rtt.onAck(src, seq, System.nanoTime());
        }
        if (pending[src].remove(seq) != null) { // removes it AND checks it was there, if already have been removed it will return null
            window[src].release();
        }
    }

    private void retransmitLoop() {
        while (running) {
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                return;
            }
            for (int dest = 1; dest < pending.length; dest++) {
                for (Message m : pending[dest].values()) {
                    if (rtt != null) {
                        rtt.onRetransmit(dest, m.seq);
                    }
                    transport.sendData(dest, m.seq, m.payload);
                }
            }
        }
    }

    void stop() {
        running = false;
        transport.close();
        retransmitter.interrupt();
        if (rtt != null) {
            rtt.close();
        }
    }
}
