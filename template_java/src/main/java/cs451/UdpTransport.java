package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;

// Packet layout:
//   DATA: [type: 1 byte][senderId: 1 byte][seq: 4 bytes][payload: 4 bytes]
//   ACK:  [type: 1 byte][senderId: 1 byte][seq: 4 bytes]
// senderId fits in 1 byte because the project caps the number of processes at 128 (§2.1).
// More than 255 processes would silently corrupt the id: widen the field first.
final class UdpTransport implements Transport {
    private static final byte DATA = 0;
    private static final byte ACK = 1;
    private static final int DATA_SIZE = 10;
    private static final int ACK_SIZE = 6;

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
    public void sendData(int dest, int seq, int payload) {
        ByteBuffer buf = ByteBuffer.allocate(DATA_SIZE);
        buf.put(DATA).put((byte) myId).putInt(seq).putInt(payload);
        send(dest, buf.array());
    }

    @Override
    public void sendAck(int dest, int seq) {
        ByteBuffer buf = ByteBuffer.allocate(ACK_SIZE);
        buf.put(ACK).put((byte) myId).putInt(seq);
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
        byte[] bytes = new byte[DATA_SIZE];
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length); //evelope that points at the buffer where the data gets written
        while (true) {
            try {
                packet.setLength(bytes.length); // Java shrinks the packet's length to the size of whatever arrived last. E.g. if ACK was send last it would only accept 6 bytes and cuts a 10-byte DATA packet short. Reseting. it prevents that
                socket.receive(packet); //blocks: the thread sleeps until a packet arrives
            } catch (IOException e) {
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(bytes, 0, packet.getLength());
            byte type = buf.get();
            int src = buf.get() & 0xFF;   // ids go up to 128, which doesn't fit in a signed byte
            int seq = buf.getInt();
            if (type == DATA) { //callback on stubborn link
                upper.onData(src, seq, buf.getInt());
            } else if (type == ACK) {
                upper.onAck(src, seq);
            }
        }
    }

    @Override
    public void close() {
        socket.close();
    }
}
