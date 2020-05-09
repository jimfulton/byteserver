=================================================
Experimental Fast ZEO/ZODB server written in Rust
=================================================

The main goal of this project is to provide a fast `ZEO
<https://github.com/zopefoundation/ZEO>`_ server for `ZODB
<http://www.zodb.org>`_.

This is in an early stage of development.

Unlike ZEO, this server treats the data it stores as opaque, as a result, it:

- Requires clients to provide conflict resolution.

- Requires use of an external garbage collector.

- Does not support undo (although it might support undo without
  conflict resolution later).

