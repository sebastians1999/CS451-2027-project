- Earlier in the same section it says "a single process only receives messages while the rest of processes only send messages." So there is always exactly one receiver, and every other process sends the same `m` messages to it.

## Later: batching

Out of scope until the first version works.

- Each UDP packet carries one message. Every `sendData`, every retransmission, and every `sendAck` sends its own packet.
- Delivering one message costs at least two packets, one DATA and one ACK. Each packet adds about 28 bytes of IP and UDP headers and one system call.
- The project allows up to 8 messages per packet, per FAQ Q3. Batching cuts the number of packets by up to eight times, and one ACK can confirm several messages.
- Open design questions:
  - Should a batch wait for 8 messages, or also go out after a short timeout?
  - What does the packet format need so the receiver knows how many messages a packet holds?
  - How does a batched ACK name the messages it confirms?
- Batching changes the packet format in `UdpTransport`. Update the layout comment, `DATA_SIZE`, and the receive buffer size with it.
