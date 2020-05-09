======
To dos
======

- Most functionality beyond simple store/load. :) pack, replication, etc.

- Something to check for invalid data.

- Lock/vote timeouts.  (Probably using the ``timer`` crate.)




- Better unmarshalling errors. tpc_begin error -> client waiting for vote.
  Basically, errors in async methods on server should disconnect.

- server logging

- Client gets invalis transaction error after reconnecting while
  waiting for vote response. It should already have aborted.

