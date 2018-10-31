package com.lxy

abstract class Cache[K, V] {
  def get(key: K): Option[V]
  def get(key: K, loader: Loader[K, V]): Option[V]
  def contains(key: K): Boolean
  def remove(key: K): Option[V]
  def size(): Long
  def status(): CacheStatus
  def clear(): Unit
}

object Cache {
  def lru[K, V](
      concurrency: Int,
      initialCapacity: Int,
      maxWeight: Long,
      weigher: Weigher[K, V],
      defaultLoader: Loader[K, V],
      cacheHandler: CacheHandler[K, V],
      removeListeners: Seq[RemoveListener[K, V]],
      lockManagement: LockManagement[K]): Cache[K, V] = {
    new LRUCache(concurrency,
        initialCapacity,
        maxWeight,
        weigher,
        defaultLoader,
        cacheHandler,
        removeListeners,
        lockManagement)
  }
  // TODO: more cache algorithms support
}
