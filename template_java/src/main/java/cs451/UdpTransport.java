package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// Packet layout, big-endian ints:
//   DATA: [type:1][senderId:1][count:1][(seq:4, payload:4) x count]
//   ACK:  [type:1][senderId:1][count:1][seq:4 x count]
// count is 1..Transport.MAX_BATCH. senderId fits in 1 byte because the project caps the number of processes at 128 (§2.1).
// More than 255 processes would silently corrupt the id: widen the field first.
final class UdpTransport implements Transport {
    private static final byte DATA = 0;
    private static final byte ACK = 1;
    private static final int HEADER_SIZE = 3;
    private static final int MAX_DATA_SIZE = HEADER_SIZE + Transport.MAX_BATCH * 8;

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
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + batch.size() * 8);
        buf.put(DATA).put((byte) myId).put((byte) batch.size());
        for (Message message : batch) {
            buf.putInt(message.seq).putInt(message.payload);
        }
        send(dest, buf.array());
    }

    @Override
    public void sendAck(int dest, int[] seqs) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + seqs.length * 4);
        buf.put(ACK).put((byte) myId).put((byte) seqs.length);
        for (int seq : seqs) {
            buf.putInt(seq);
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
            byte type = buf.get();
            int src = buf.get() & 0xFF;   // ids go up to 128, which doesn't fit in a signed byte
            int count = buf.get() & 0xFF;
            if (type == DATA) { //callback on stubborn link
                List<Message> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    batch.add(new Message(buf.getInt(), buf.getInt()));
                }
                upper.onData(src, batch);
            } else if (type == ACK) {
                int[] seqs = new int[count];
                for (int i = 0; i < count; i++) {
                    seqs[i] = buf.getInt();
                }
                upper.onAck(src, seqs);
            }
        }
    }

    @Override
    public void close() {
        socket.close();
    }
}
