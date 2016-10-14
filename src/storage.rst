=========================
Filestorage2 design notes
=========================

See: https://github.com/jimfulton/filestorage2

.. contents::

Overview
========

This module provides an implementation of a file storage suitable for
use in a byteserver.  It is simplar in functionality to a `ZODB
storage
<http://www.zodb.org/en/latest/reference/storages.html#istorage>`_,
but has a narrower scope:

- Not used directly as a storage for an object database

- Not pluggable, but focussed on performance.

The design is influenced by experience with `ZEO
<https://github.com/zopefoundation/ZEO>`_:

- Unlike ZEO, use object level locks to allow non-overlapping
  transactions to be active right up to the final commit.

- Allow parallel reads.  All reads are for data written some time in
  the past so there's no need to disallow reading while writing.

- Pool (and truncate after use) temporary files to avoid file-creation overhead.

The implementation will be incremental. The initial focus is on
writing and reading data transactionally.  Other features, pack (segments) and
like replication will be added over time.

- There will be a thread safe storage object shared (Via Arc) among
  multiple threades.

- Read operations will use a read-file pool, so multiple threads can
  read at once.

- When a transaction is begin, a temporary file will be fetched from a
  pool and managed by the transaction.  It will be used to log data
  received during the first phase of two-phase commit.

Format
======

Files
-----

- Active file, with name NAME

  All other files have name as NAME + '.' + extension.

- New active file, extension: 'new-active'.

  This exists only during split.

- Index file, extension: 'index'

- Previous files, extension: hex end tid.

- Previous file indxes, extension: hex end tid + '.index'.

- temporary files, extension 'tmpN', where N is a number from 0
  through temporary-file pool size.

- lock file, extension: 'lock'

Split and packing
=================

Split
-----

Splits active file into a previous file and a new active file.

- Current active must be non-empty.

- Create a new active file with temporary extension, 'new-active'.

- Create a new in-memory index for new active file, adding it to the
  front of the index list.

- In separate thread.

  - Rename current active file with last tid as
    extension, making it a new previous file.

  - Rename new active file removing 'new-active extension'.

  - Write index for new previous/old active file.

- On startup, check for new-active file and finish split operation, if
  necessary.

Packing
-------

Merges 2 or more previous files.

New file as previous-file-tid of first file and name of last file.

Old files are renamed by adding packYYMMDDSS.SSSSSS extension, based
on time when pack was completed.

Note that there is no-longer a pack time per se.  Merged-file records
are most current within the set.

Objects
-------

FileStorage
  Active Segment
    file
    index
  Previous Segment *
    file
    index
