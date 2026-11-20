package cs451.perfectlinks;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cs451.Deliverer;
import cs451.Host;

public final class PerfectLink implements Deliverer {
    private final Deliverer upper;
    // Only the receive thread touches these, so they need no synchronization.
    private final int[] contiguousUpTo;
    private final List<HashSet<Integer>> outOfOrder;   // indexed by host id; index 0 unused
    private final StubbornLink sl;

    public PerfectLink(int myId, List<Host> hosts, LinkConfig config, Deliverer upper) throws SocketException {
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

    public void send(int dest, int seq, int payload) throws InterruptedException {
        sl.send(dest, seq, payload);
    }

    public void flush(int dest) {
        sl.flush(dest);
    }

    @Override
    public boolean deliver(int src, int seq, int payload) {
        if (alreadyDelivered(src, seq)) {
            return true; // duplicate still has to be acked, or the sender retransmits forever
        }
        // Record only after the upper layer accepts. A refusal must look new on retransmit.
        if (!upper.deliver(src, seq, payload)) {
            return false;
        }
        outOfOrder.get(src).add(seq); //if one message does not ever get delivered we are stuck emptying the set but we still keep on delivering
        extendRun(src);
        return true;
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

    public void stop() { //shutdown entrypoint when main receives SIGINT or SIGTERM
        sl.stop();
    }
}
