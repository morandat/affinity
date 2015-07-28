# Java Thread Affinity

This is self-contained library (one file, which only depends on JNA) allows to create pool of threads which have affinity with differents cores.

Currently it only works on HotSpot and Linux (but MacOS should be supported by this night ;) )

```
long mask = ThreadAffinity.getProcessAffinityMask();
ExecutorService threadGroupSingle = Executors.newFixedThreadPool(Long.bitCount(mask), new ThreadAffinity.SingleCPUThreadFactory(mask));
// or
ExecutorService threadGroupRandom = Executors.newFixedThreadPool(Long.bitCount(mask), new ThreadAffinity.AffinityThreadFactory(mask));
```
