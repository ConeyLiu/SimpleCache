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
  * A checker that will check the conditions when put store the loading value.
  * It'll evict the entries until the conditions are met.
  */
trait Satisfy[K, V] {
  /**
    * Check whether the conditions are satisfied.
    */
  def check(key: K, value: V, weight: Int): Boolean
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