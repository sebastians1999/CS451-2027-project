package cs451.perfectlinks;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import cs451.Host;

// Packet layout, big-endian:
//   DATA: [type:1][senderId:1][count:1][(linkSeq:4, origin:1, seq:4, payload:4) x count]
//   ACK:  [type:1][senderId:1][count:1][linkSeq:4 x count]
// count is 1..Transport.MAX_BATCH. senderId and origin fit in 1 byte because the project caps processes at 128 (§2.1).
// More than 255 processes would silently corrupt the id: widen the field first.
final class UdpTransport implements Transport {
    private static final byte DATA = 0;
    private static final byte ACK = 1;
    private static final int HEADER_SIZE = 3;
    private static final int DATA_ENTRY_SIZE = 13; // linkSeq 4 + origin 1 + seq 4 + payload 4
    private static final int MAX_DATA_SIZE = HEADER_SIZE + Transport.MAX_BATCH * DATA_ENTRY_SIZE;

    private final int myId;
    private final DatagramSocket socket;
    private final InetSocketAddress[] addresses;   // indexed by host id
    private final PacketHandler upper;

    UdpTransport(int myId, List<Host> hosts, PacketHandler upper) throws SocketException {
        this.myId = myId;
        this.upper = upper;
        this.addresses = new InetSocketAddress[hosts.size() + 1];
        for (Host h : hosts) {
            addresses[h.getId()] = new InetSocketAddress(h.getIp(), h.getPort());
        }
        this.socket = new DatagramSocket(addresses[myId].getPort());
    }

    @Override
    public void sendData(int dest, List<Message> batch) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + batch.size() * DATA_ENTRY_SIZE);
        buf.put(DATA).put((byte) myId).put((byte) batch.size());
        for (Message message : batch) {
            buf.putInt(message.linkSeq).put((byte) message.origin).putInt(message.seq).putInt(message.payload);
        }
        send(dest, buf.array());
    }

    @Override
    public void sendAck(int dest, int[] linkSeqs) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + linkSeqs.length * 4);
        buf.put(ACK).put((byte) myId).put((byte) linkSeqs.length);
        for (int linkSeq : linkSeqs) {
            buf.putInt(linkSeq);
        }
        send(dest, buf.array());
    }

    private void send(int dest, byte[] bytes) {
        try {
            socket.send(new DatagramPacket(bytes, bytes.length, addresses[dest]));
        } catch (IOException e) {
            // socket closed during shutdown, or a transient error: the stubborn link retransmits anyway
        }
    }

    @Override
    public void start() {
        new Thread(this::receiveLoop, "receive").start();
    }

    private void receiveLoop() {
        byte[] bytes = new byte[MAX_DATA_SIZE];
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length); //evelope that points at the buffer where the data gets written
        while (true) {
            try {
                packet.setLength(bytes.length); // Java shrinks the packet's length to the size of whatever arrived last. A short ACK would then truncate the next DATA packet. Resetting it prevents that.
                socket.receive(packet); //blocks: the thread sleeps until a packet arrives
            } catch (IOException e) {
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(bytes, 0, packet.getLength());
            // A short or nonsense datagram is dropped. Reading past the buffer would kill this thread.
            if (buf.remaining() < HEADER_SIZE) {
                continue;
            }
            byte type = buf.get();
            int src = buf.get() & 0xFF;   // ids go up to 128, which doesn't fit in a signed byte
            int count = buf.get() & 0xFF;
            if (src < 1 || src >= addresses.length || count < 1 || count > Transport.MAX_BATCH) {
                continue;
            }
            if (type == DATA) { //callback on stubborn link
                if (buf.remaining() < count * DATA_ENTRY_SIZE) {
                    continue;
                }
                List<Message> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    int linkSeq = buf.getInt();
                    int origin = buf.get() & 0xFF;
                    int seq = buf.getInt();
                    int payload = buf.getInt();
                    batch.add(new Message(linkSeq, origin, seq, payload));
                }
                upper.onData(src, batch);
            } else if (type == ACK) {
                if (buf.remaining() < count * 4) {
                    continue;
                }
                int[] linkSeqs = new int[count];
                for (int i = 0; i < count; i++) {
                    linkSeqs[i] = buf.getInt();
                }
                upper.onAck(src, linkSeqs);
            }
        }
    }

    @Override
    public void close() {
        socket.close();
    }
}
