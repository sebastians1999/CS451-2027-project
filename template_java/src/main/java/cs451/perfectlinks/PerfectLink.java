package cs451.perfectlinks;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cs451.Deliverer;
import cs451.Host;

public final class PerfectLink {
    private final int myId;
    private final Deliverer upper;
    // Only the receive thread touches these, so they need no synchronization.
    private final int[] contiguousUpTo;
    private final List<HashSet<Integer>> outOfOrder;   // indexed by host id; index 0 unused
    private final int[] nextLinkSeq;                    // per destination; first value handed out is 1
    private final StubbornLink sl;

    public PerfectLink(int myId, List<Host> hosts, LinkConfig config, Deliverer upper) throws SocketException {
        int numHosts = hosts.size();
        this.myId = myId;
        this.upper = upper;
        this.contiguousUpTo = new int[numHosts + 1];
        this.outOfOrder = new ArrayList<>(numHosts + 1);
        this.nextLinkSeq = new int[numHosts + 1];
        for (int id = 0; id <= numHosts; id++) {
            outOfOrder.add(new HashSet<>());
        }
        // Last: the stubborn link starts receiving immediately and calls deliver().
        this.sl = new StubbornLink(myId, hosts, config, this);
    }

    public void send(int dest, int seq, int payload) throws InterruptedException {
        send(dest, myId, seq, payload);
    }

    public void send(int dest, int origin, int seq, int payload) throws InterruptedException {
        // Taken before the window can block, then always passed down, so this exact seq is what gets sent.
        int linkSeq = ++nextLinkSeq[dest];
        sl.send(dest, linkSeq, origin, seq, payload);
    }

    public void flush(int dest) {
        sl.flush(dest);
    }

    // Dedup is on (relayer, linkSeq). The link seq is not passed upward.
    boolean deliver(int relayer, int linkSeq, int origin, int seq, int payload) {
        if (alreadyDelivered(relayer, linkSeq)) {
            return true; // duplicate still has to be acked, or the sender retransmits forever
        }
        // Record only after the upper layer accepts. A refusal must look new on retransmit.
        if (!upper.deliver(relayer, origin, seq, payload)) {
            return false;
        }
        outOfOrder.get(relayer).add(linkSeq); //if one message does not ever get delivered we are stuck emptying the set but we still keep on delivering
        extendRun(relayer);
        return true;
    }

    private boolean alreadyDelivered(int relayer, int linkSeq) {
        return linkSeq <= contiguousUpTo[relayer] || outOfOrder.get(relayer).contains(linkSeq);
    }

    private void extendRun(int relayer) {
        int next = contiguousUpTo[relayer] + 1;
        while (outOfOrder.get(relayer).remove(next)) {
            contiguousUpTo[relayer] = next;
            next++;
        }
    }

    public void stop() { //shutdown entrypoint when main receives SIGINT or SIGTERM
        sl.stop();
    }
}
