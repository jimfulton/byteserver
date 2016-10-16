===================
Server architecture
===================

There is a main thread that listens for connections.

For each client, there are two threads, and a channel (and a
TcpStream, of course):

reader
  The reader thread reads incoming requests.

  For read requests, it performs requests and then sends results over
  the channel to the writer to send the results back to the client.

  Write requests are forwarded to the writer (below) over the channel.

writer
  The writer writes data back to the client.  It also manages transaction.

  It receives results from the reader over the channel and sends them
  to the client over the TcpStream.

  It also gets write requests forwarded from the reader, which it acts
  on itself.

  In the course of managing transactions, it gets locking and
  transaction-finish notifications over the channel.  It sends
  responses to non-asyncchronous write requests (vote, tcp_finish,
  tcp_abort) to the client over the TcpStream.  Vote and finish
  responses are delayed.

Only readers read from the TcpStreams and only writers write to
TcpStreams. Synchronization of readers and writers happens through
their channels.

Transaction locking happens in the storage using object-level
locks. When clients successfully vote, their data is written to the
database but marked as provisional.  When the client sends a
tpc_finish request, the transaction records are marked as final,
however, notifications may be delayed.

Transaction ids are assigned when votes succeed.  This means that
there's a queue, in transaction id order, of voted transactions
waiting to be finished.  When a transaction is finished, it's marked
as such in the queue.  When a finished transaction reaches the front
of the queue, the database index is updated and notifications are sent
to clients, including the committing client.
