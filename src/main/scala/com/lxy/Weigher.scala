package com.lxy

trait Weigher[K, V] {
  def weight(key: K, value: V): Int
}
