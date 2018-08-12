package com.lxy

/**
  * A checker that will check the conditions when put store the loading value.
  * It'll evict the entries until the conditions are met.
  */
trait Satisfy[K, V] {
  /**
    * Check whether the conditions are satisfied
    */
  def check(key: K, value: V, weight: Int): Boolean
}
