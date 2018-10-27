# SimpleCache

This is a special LRU cache for cache data into OFF_HEAP memory. Normally, we should manage the OFF_HEAP accurately, however there still some fragmentation with memory management(such as jemalloc) in now days. So this LRU cache is designed for this case, we will evict the entry when we can't request enough memory.