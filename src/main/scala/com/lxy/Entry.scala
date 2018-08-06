package com.lxy

import scala.reflect.ClassTag

trait Entry[K, V] {
  def keyClassTag: ClassTag[K]
  def valueClassTag: ClassTag[V]
  def setValueReference(reference: ValueReference[K, V]): Unit
  def getValueReference(): ValueReference[K, V]
  def getKey(): K
  def getHash(): Int
  def getNext(): Entry[K, V]
  def setNext(entry: Entry[K, V]): Unit
  def setNextInAccessQueue(next: Entry[K, V]): Unit
  def getNextInAccessQueue(): Entry[K, V]
  def setPreviousInAccessQueue(previous: Entry[K, V]): Unit
  def getPreviousInAccessQueue(): Entry[K, V]
}

abstract class AbstractEntry[K, V] extends Entry[K, V] {
  override def keyClassTag: ClassTag[K] = throw new UnsupportedOperationException

  override def valueClassTag: ClassTag[V] = throw new UnsupportedOperationException

  override def setValueReference(reference: ValueReference[K, V]): Unit = throw new UnsupportedOperationException

  override def getValueReference(): ValueReference[K, V] = throw new UnsupportedOperationException

  override def getKey(): K = throw new UnsupportedOperationException

  override def getHash(): Int = throw new UnsupportedOperationException

  override def getNext(): Entry[K, V] = throw new UnsupportedOperationException

  override def setNext(entry: Entry[K, V]): Unit = throw new UnsupportedOperationException

  override def setNextInAccessQueue(next: Entry[K, V]): Unit = throw new UnsupportedOperationException

  override def getNextInAccessQueue(): Entry[K, V] = throw new UnsupportedOperationException

  override def setPreviousInAccessQueue(previous: Entry[K, V]): Unit = throw new UnsupportedOperationException

  override def getPreviousInAccessQueue(): Entry[K, V] = throw new UnsupportedOperationException
}

class ValueReferenceEntry[K, V](
    key: K,
    hash: Int,
    private var next: Entry[K ,V],
    override val keyClassTag: ClassTag[K],
    override val valueClassTag: ClassTag[V]) extends AbstractEntry[K, V] {

  private var reference: ValueReference[K, V] = null
  private var nextAccess: Entry[K, V] = Entry.getNullEntry[K, V]()
  private var previousAccess: Entry[K, V] = Entry.getNullEntry[K, V]()

  override def setValueReference(reference: ValueReference[K, V]): Unit = {
    this.reference = reference
  }

  override def getValueReference(): ValueReference[K, V] = reference

  override def getKey(): K = key

  override def getHash(): Int = hash

  override def getNext(): Entry[K, V] = next

  override def setNext(entry: Entry[K, V]): Unit = {
    next = entry
  }

  override def setNextInAccessQueue(next: Entry[K, V]): Unit = {
    this.nextAccess = next
  }

  override def getNextInAccessQueue(): Entry[K, V] = nextAccess

  override def setPreviousInAccessQueue(previous: Entry[K, V]): Unit = {
    this.previousAccess = previous
  }

  override def getPreviousInAccessQueue(): Entry[K, V] = previousAccess
}

object Entry {

  private val NULL = new ValueReferenceEntry[Any, Any](0, 0, null, null, null)

  def getNullEntry[K, V](): Entry[K, V] = {
    NULL.asInstanceOf[Entry[K, V]]
  }

  def apply[K: ClassTag, V: ClassTag](
      key: K,
      hash: Int,
      next: Entry[K, V]): Entry[K, V] =
    new ValueReferenceEntry(key, hash, next, implicitly[ClassTag[K]], implicitly[ClassTag[V]])
}