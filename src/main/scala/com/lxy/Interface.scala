package com.lxy

/**
 * Listener which will be called when a entry is removed.
 */
trait RemoveListener[K, V] {
  /**
   * The method will be called when a entry is removed from the cache.
   * @param key the key of removed entry
   * @param value the value of removed entry
   */
  def onRemove(key: K, value: V): Unit
}

/**
 * A cache handler used to allocate the requested memory and cache the data into the given address.
 * Ideally, we should requested the memory first and load the data into the address directly. However,
 * firstly, we may support cache data asynchronously. Secondly, we should get the accurate space for
 * the data cache.
 *
 * So, the step of cache data as follows:
 *
 *     val value = loader.load(key)
 *     val weight = weigher.weight(key, value)
 *     var address = cacheHandler.allocate(key, value, weight)
 *     if (address != 0) {
 *        cacheHandler.cache(key, value, weight, address)
 *     } else {
 *        // the weight should be enough, however there may be memory fragmentation.
 *        // So, we need evict some data ...
 *     }
 * @tparam K
 * @tparam V
 */
trait CacheHandler[K, V] {

  /**
   * @return the address of allocated memory
   */
  def allocate(key: K, value: V, weight: Int): Long

  def cache(key: K, value: V, weight: Int, address: Long): Boolean
}

/**
 * A weigher that used to calculate the quantify of a entry which is defined by the key and the value.
 * @tparam K the key of the entry
 * @tparam V the value of the entry
 */
trait Weigher[K, V] {
  /**
   * Calculate the weight with given key and value.
   * @return the target weight
   */
  def weight(key: K, value: V): Int
}

/**
 * A loader that used to load a cache missed value with the given method.
 */
trait Loader[K, V] {
  /**
   * Loading the given key by this method.
   * @param key the key which need to load
   * @return the loaded value
   */
  def load(key: K): V
}

trait SafeLoader[K, V] {

}