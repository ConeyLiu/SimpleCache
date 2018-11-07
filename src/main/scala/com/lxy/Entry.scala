package com.lxy

import java.util.Objects

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

  override def hashCode(): Int = Objects.hashCode(key, hash)

  override def equals(obj: Any): Boolean = obj match {
    case other: ConcreteEntry[K, V] =>
      getHash() == other.getHash() &&
        getKey() == other.getKey() &&
        hashCode() == other.hashCode()
    case _ => false
  }

  override def toString: String = {
    s"{Key: ${key}, " +
      s"hash: ${hash}, " +
      s"value: ${value}, " +
      s"weight: ${weight}, " +
      s"next key: ${if (next == null) "null" else next.getKey()}}"
  }
}

object Entry {

  private val NULL = new AbstractEntry[Any, Any] {
    override def getKey(): Any = 0

    override def getHash(): Int = 0

    override def getValue(): Any = 0

    override def getWeight: Int = 0

    override def getNext(): Entry[Any, Any] = null

    override def setNextInAccessQueue(next: Entry[Any, Any]): Unit = {}

    override def getNextInAccessQueue(): Entry[Any, Any] = null

    override def setPreviousInAccessQueue(previous: Entry[Any, Any]): Unit = {}

    override def getPreviousInAccessQueue(): Entry[Any, Any] = null
  }

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
    require(original != null, "The original entry can't be null")
    // here, we can't nullify the access order in original access queue, because we need guarantee that
    // the read of old table can be proceed
    val entry = new ConcreteEntry[K, V](original.getKey(), original.getHash(),
      original.getValue(), original.getWeight, newNext)
    entry.setPreviousInAccessQueue(original.getPreviousInAccessQueue())
    entry.setNextInAccessQueue(original.getNextInAccessQueue())
    entry
  }
}