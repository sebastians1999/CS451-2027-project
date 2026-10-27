package cs451;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

final class PerfectLink implements Deliverer {
    private final Deliverer upper;
    // Only the receive thread touches these, so they need no synchronization.
    private final int[] contiguousUpTo;
    private final List<HashSet<Integer>> outOfOrder;   // indexed by host id; index 0 unused
    private final StubbornLink sl;

    PerfectLink(int myId, List<Host> hosts, LinkConfig config, Deliverer upper) throws SocketException {
        int numHosts = hosts.size();
        this.upper = upper;
        this.contiguousUpTo = new int[numHosts + 1];
        this.outOfOrder = new ArrayList<>(numHosts + 1);
        for (int id = 0; id <= numHosts; id++) {
            outOfOrder.add(new HashSet<>());
        }
        // Last: the stubborn link starts receiving immediately and calls deliver().
        this.sl = new StubbornLink(myId, hosts, config, this);
    }

    void send(int dest, int seq, int payload) throws InterruptedException {
        sl.send(dest, seq, payload);
    }

    void flush(int dest) {
        sl.flush(dest);
    }

    @Override
    public void deliver(int src, int seq, int payload) {
        if (alreadyDelivered(src, seq)) {
            return;
        }
        outOfOrder.get(src).add(seq); //if one message does not ever get delivered we are stuck emptying the set but we still keep on delivering
        extendRun(src);
        upper.deliver(src, seq, payload);
    }

    private boolean alreadyDelivered(int src, int seq) {
        return seq <= contiguousUpTo[src] || outOfOrder.get(src).contains(seq);
    }

    private void extendRun(int src) {
        int next = contiguousUpTo[src] + 1;
        while (outOfOrder.get(src).remove(next)) {
            contiguousUpTo[src] = next;
            next++;
        }
    }

    void stop() { //shutdown entrypoint when main receives SIGINT or SIGTERM
        sl.stop();
    }
}
