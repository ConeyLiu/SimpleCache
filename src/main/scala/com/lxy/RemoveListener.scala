package com.lxy

trait RemoveListener[K, V] {
  def onRemove(key: K, value: V): Unit
}
