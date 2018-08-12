package com.lxy

import scala.reflect.ClassTag

abstract class Cache[K: ClassTag, V: ClassTag] {
  def get(key: K): Option[V]
  def get(key: K, loader: Loader[K, V]): Option[V]
  def contains(key: K): Boolean
  def remove(key: K): Option[V]
  protected def reHash(key: K): Int
  private[lxy] def enqueueNotification(entry: Entry[K, V]): Unit
  def clear(): Unit
}

object Cache {
  def lru[K, V](
      concurrency: Int,
      initialCapacity: Int,
      maxWeight: Long,
      weigher: Weigher[K, V],
      defaultLoader: Loader[K, V],
      removeListeners: Seq[RemoveListener[K, V]]): Cache[K, V] = {
    new LRUCache(concurrency, initialCapacity, maxWeight, weigher, defaultLoader, removeListeners)
  }
}
