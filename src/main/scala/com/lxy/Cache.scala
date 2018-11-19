package com.lxy

abstract class Cache[K, V, R] {
  def get(key: K): Option[R]
  def get(key: K, loader: Loader[K, V]): Option[R]
  def getKeys(): Set[K]
  def contains(key: K): Boolean
  def remove(key: K): Option[R]
  def size(): Long
  def status(): CacheStatus
  def clear(): Unit
}

object Cache {
  /**
   * Build a LRU cache.
   * @param concurrency the maximum concurrency for the cache, if the value is not aligned with 8,
   *                    we will modify it align to 8
   * @param initialCapacity the totally initial capacity for the cache, each segment initial
   *                        capacity should be initialCapacity/concurrency_aligned and then align
   *                        to 8
   * @param maxWeight the maximum weight for the total cache, each segment should be
   *                  maxWeight / concurrency_aligned or maxWeight / concurrency_aligned + 1
   * @param weigher used to calculate the weight for given key and value
   * @param defaultLoader the default loader which used to load value with given key
   * @param cacheHandler after loaded value, we used this cacheHandler to cache the given value to
   *                     given address, so the handler need to request memory. Details usage please
   *                     see the code.
   * @param removeListeners a list of listeners will be incurred when a entry is evicted or removed
   * @param forceRemoveListeners this list of listeners are incurred by current thread which
   *                             different from the removeListeners that called by a seperated
   *                             scheduled thread
   * @tparam K the key type
   * @tparam V the value type
   * @tparam R the cached value type, after loader value for given key, we need to cache it to R
   *           type
   */
  def lru[K, V, R](
      concurrency: Int,
      initialCapacity: Int,
      maxWeight: Long,
      weigher: Weigher[K, V],
      defaultLoader: Loader[K, V],
      cacheHandler: CacheHandler[K, V, R],
      removeListeners: Seq[RemoveListener[K, R]],
      forceRemoveListeners: Seq[RemoveListener[K, R]]): Cache[K, V, R] = {
    new LRUCache[K, V, R](concurrency,
      initialCapacity,
      maxWeight,
      weigher,
      defaultLoader,
      cacheHandler,
      removeListeners,
      forceRemoveListeners)
  }
  // TODO: more cache algorithms support
}

