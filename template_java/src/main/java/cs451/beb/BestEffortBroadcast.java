package cs451.beb;

import java.net.SocketException;
import java.util.List;

import cs451.Deliverer;
import cs451.Host;
import cs451.perfectlinks.LinkConfig;
import cs451.perfectlinks.PerfectLink;

public final class BestEffortBroadcast implements Deliverer {
    private final List<Host> hosts;
    private final Deliverer upper;
    private final PerfectLink pl;

    public BestEffortBroadcast(int myId, List<Host> hosts, LinkConfig config, Deliverer upper)
            throws SocketException {
        this.hosts = hosts;
        this.upper = upper;
        // The link starts receiving before its constructor returns and can call deliver.
        this.pl = new PerfectLink(myId, hosts, config, this);
    }

    public void broadcast(int seq, int payload) throws InterruptedException {
        for (Host host : hosts) {
            // Self is included: a send to our own id is a normal datagram.
            // No flush: the stubborn link batches up to 8, and its retransmit
            // thread sends anything still sitting in the batch.
            pl.send(host.getId(), seq, payload);
        }
    }

    @Override
    public void deliver(int src, int seq, int payload) {
        upper.deliver(src, seq, payload);
    }
}
