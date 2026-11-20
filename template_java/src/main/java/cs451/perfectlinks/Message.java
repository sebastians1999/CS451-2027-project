package cs451.perfectlinks;

final class Message {
    final int seq;
    final int payload;

    Message(int seq, int payload) {
        this.seq = seq;
        this.payload = payload;
    }
}
