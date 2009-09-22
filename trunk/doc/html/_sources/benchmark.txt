Benchmark
=========

Optimizing a work-flow is a difficult task. The following examples should guide
your struggles and explain what is going on under the hood.

Grepping a file
---------------

Grepping means to search patterns in a text file based on regular expressions.
It can be thought of as a data-reduction task i.e. given a large file extract 
the relevant information. Intepreted languages like Python are typically used 
for this kid of jobs e.g. parsing specific information from a log file. In this
example we will count SNP identifiers in a large ~1Gb text file. This is a quite
IO-heavy task as a Gb of data has to be loaded into memory, but the search based
on regular expressions is also computationally intensive, as such it is a
difficult task to optimize as the overall performance will depend on variables
like: hardware (relative CPU to IO speed), operating system (how efficient is 
virtual memory?) and current system load/ memory state.

It is difficult to speed up file parsing using networked hosts as sending parts
of the text file over a network might be slower then processing them locally, so
we will stick with the locally availble CPUs. But the problem of data-sharing
between processes still remains. PaPy uses ``multiprocessing``, which 
accomplishes this via locked pipes through which pickled data-streams are
pumped. First a file has to be split into chunks and those chunks are serialized
sent through the pipe and deserialized by the parallel workers. This is not
optimal because:

  * we have to keep whole chunks in memory, possibly in several copies at once
    (initial, serialized, deserialized) because of the "random" nature of garbage
    collection.
  * pickling is not free
  * pushing data through pipes is neither

PaPy has built-in workers which allow to considerably speed up grepping of large
files by eliminating the need to pass file data through pipes. If a process is
forked the child inherits file handles from the parent. This means that the
parent process, which dispatches the chunks among child worker processes needs
to communicate only the chunk-start and chunk-stop of the open file. This is
done using the chunk_file worker::

    chunker = workers.io.chunk_file(fh, 1024)) # 2**20 is 1M
    chunker.next()
    (1, 0, 1013)

The chunker will yield chunk boundaries (the first and the last byte) and the
associated file descriptor to the provided file handle. The size of the chunk in
bytes will only pproximate the provided size argument as the chunk_file worker
splits only at new-line '\n' characters.





