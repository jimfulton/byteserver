===================
Byteserver protocol
===================

Wire
====

Sequence of sized messages:

- big-endian 32-bit message size

- Data

First message is protocol identifier.

Remaining messages are msgpack-encoded triples:

message id
   -1 => heartbeat
   0  => async message, no reply should be sent.
   >0 => id to send response with.

method
  'E' => message is an error result
  'R' => message is a normal reply

payload
  request  => tuple of arguments
  error => tuple of error name and error data, where data is a tuplle
           of values.
  response => returned value

Methods
=======

Methods are synchronous unless otherwise noted.

register(storage, read_only)
  Register to use a particular storage.

  This must be the first message sent.

  It returns the last committed transaction id.

loadBefore(oid, tid)
  Load the value for oid committed before Tid.

