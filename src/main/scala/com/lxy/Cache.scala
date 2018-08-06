package com.lxy

import scala.reflect.ClassTag

abstract class Cache[K: ClassTag, V: ClassTag] {
  def get(key: K): V
  def contains(key: K): Boolean
  def remove(key: K): Boolean
  def clear(): Unit
}

object Cache {

}
