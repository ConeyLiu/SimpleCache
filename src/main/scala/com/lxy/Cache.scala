package com.lxy

abstract class Cache[K, V] {
  def get(key: K): Option[V]
  def get(key: K, loader: Loader[K, V]): Option[V]
  def contains(key: K): Boolean
  def remove(key: K): Option[V]
  protected def reHash(key: K): Int
  protected def enqueueNotification(entry: Entry[K, V]): Unit
  protected def triggerListenerManual(entry: Entry[K, V]): Unit
  def clear(): Unit
}

object Cache {
  def lru[K, V](
      concurrency: Int,
      initialCapacity: Int,
      maxWeight: Long,
      weigher: Weigher[K, V],
      defaultLoader: Loader[K, V],
      satisfy: Satisfy[K, V],
      removeListeners: Seq[RemoveListener[K, V]]): Cache[K, V] = {
    new LRUCache(concurrency, initialCapacity, maxWeight, weigher, defaultLoader, satisfy, removeListeners)
  }

  // TODO: more cache algorithms support
}
