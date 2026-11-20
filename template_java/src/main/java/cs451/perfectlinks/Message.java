package cs451.perfectlinks;

final class Message {
    final int linkSeq;
    final int origin;
    final int seq;
    final int payload;

    Message(int linkSeq, int origin, int seq, int payload) {
        this.linkSeq = linkSeq;
        this.origin = origin;
        this.seq = seq;
        this.payload = payload;
    }
}
