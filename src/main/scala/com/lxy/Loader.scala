package com.lxy

abstract class Loader[K, V] {
  def load(key: K): V
}
