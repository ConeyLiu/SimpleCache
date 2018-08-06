package com.lxy

import scala.reflect.ClassTag

abstract class Loader[K: ClassTag, V: ClassTag] {
  def load(key: K): V
}
