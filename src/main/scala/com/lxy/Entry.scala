package com.lxy

trait Entry[K, V] {
  def getKey(): K
  def getHash(): Int
  def getValue(): V
  def getWeight: Int
  def getNext(): Entry[K, V]
  def setNextInAccessQueue(next: Entry[K, V]): Unit
  def getNextInAccessQueue(): Entry[K, V]
  def setPreviousInAccessQueue(previous: Entry[K, V]): Unit
  def getPreviousInAccessQueue(): Entry[K, V]
}

abstract class AbstractEntry[K, V] extends Entry[K, V] {

  override def getKey(): K = throw new UnsupportedOperationException

  override def getHash(): Int = throw new UnsupportedOperationException

  override def getValue(): V = throw new UnsupportedOperationException

  override def getWeight: Int = throw new UnsupportedOperationException

  override def getNext(): Entry[K, V] = throw new UnsupportedOperationException

  override def setNextInAccessQueue(next: Entry[K, V]): Unit = throw new UnsupportedOperationException

  override def getNextInAccessQueue(): Entry[K, V] = throw new UnsupportedOperationException

  override def setPreviousInAccessQueue(previous: Entry[K, V]): Unit = throw new UnsupportedOperationException

  override def getPreviousInAccessQueue(): Entry[K, V] = throw new UnsupportedOperationException
}

class ConcreteEntry[K, V](
    private final val key: K,
    private final val hash: Int,
    private final val value: V,
    private final val weight: Int,
    private final val next: Entry[K ,V]) extends AbstractEntry[K, V] {

  @volatile private var nextAccess: Entry[K, V] = Entry.getNullEntry[K, V]()
  @volatile private var previousAccess: Entry[K, V] = Entry.getNullEntry[K, V]()

  override def getKey(): K = key

  override def getHash(): Int = hash

  override def getValue(): V = value

  override def getWeight: Int = weight

  override def getNext(): Entry[K, V] = next

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

  private val NULL = new ConcreteEntry[Any, Any](0, 0, null, 0, null)

  def getNullEntry[K, V](): Entry[K, V] = {
    NULL.asInstanceOf[Entry[K, V]]
  }

  def apply[K, V](
      key: K,
      hash: Int,
      value: V,
      weight: Int,
      next: Entry[K, V]): Entry[K, V] =
    new ConcreteEntry(key, hash, value, weight, next)

  def copy[K, V](
      original: Entry[K, V],
      newNext: Entry[K, V]): Entry[K, V] = {
    // here, we can't null the access entry in original access queue, because we need guarantee that
    // the read of old table can proceed
    val entry = new ConcreteEntry[K, V](original.getKey(), original.getHash(),
      original.getValue(), original.getWeight, newNext)
    entry.setPreviousInAccessQueue(original.getPreviousInAccessQueue())
    entry.setNextInAccessQueue(entry.getNextInAccessQueue())
    entry
  }
}