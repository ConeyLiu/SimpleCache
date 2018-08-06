package com.lxy

import java.util
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import scala.reflect.ClassTag

class LRUCache[K: ClassTag, V: ClassTag] extends Cache[K, V] {
  override def get(key: K): V = ???

  private def put(key: )

  override def contains(key: K): Boolean = ???

  override def remove(key: K): Boolean = ???

  override def clear(): Unit = ???
}

private[lxy] class Segment[K: ClassTag, V: ClassTag](
    length: Int,
    maxWeight: Long,
    defaultLoader: Loader[K, V],
    weigher: Weigher[K, V],
    removeListeners: Seq[RemoveListener[K, V]]){
  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()
  @volatile private var totalWeight: Long = 0L
  @volatile private var table = new AtomicReferenceArray[Entry[K, V]](length)
  @volatile private var accessQueue = new AccessQueue[K, V]()

  def get(key: K, hash: Int): V = {
    get(key, hash, defaultLoader)
  }

  def get(key: K, hash: Int, loader: Loader[K, V]): V = {
    var entry: Entry[K, V] = null
    readLock.lock()
    try {
      val first = table.get((hash & (table.length() - 1)))
      entry = first
      while (entry != null && entry.getKey() != key) {
        entry = entry.getNext()
      }

      if (entry != null) {
        val valueReference = entry.getValueReference()
        if (valueReference.isActive()) {
          val value = valueReference.getValue()
          require(value.isDefined, "value is not defined")
          recordRead(entry)
          return value.get
        } else {
          // Currently, this is not supported
          throw new UnsupportedOperationException("This is only supported in loading with async")
        }
      } else {
        put(key, hash, first, defaultLoader)
      }
    } finally {
      readLock.unlock()
    }
  }

  private def put(key: K, hash: Int, firstEntry: Entry[K, V], loader: Loader[K, V]): V = {
    writeLock.lock()
    try {
      val newEntry = Entry(key, hash, firstEntry)
      val value = loader.load(key)
      val weight = weigher.weight(key, value)
      if (weight > (maxWeight - totalWeight)) {
        // if there aren't enough space, evict it.
        evict(maxWeight - weight)
      }
      totalWeight += weight
      val valueReference = new NormalValueReference[K, V](value, weight)
      newEntry.setValueReference(valueReference)
      table.set((hash & (table.length() - 1)), newEntry)
      recordRead(newEntry)
      value
    } finally {
      writeLock.unlock()
    }
  }

  private def evict(max: Long): Unit = {
    while (totalWeight > max) {
      val evictedEntry = accessQueue.poll()
      val reference = evictedEntry.getValueReference()
      if (!reference.isActive()) {
        removeEntryFromChain(evictedEntry)
        removeListeners.foreach(f => f(evictedEntry.getKey(), reference.getValue()))
      }
    }
  }

  private def removeEntryFromChain(entry: Entry[K, V]): Boolean = {
    writeLock.lock()
    try {
      val hash = entry.getHash()
      val first = table.get((hash & (table.length() - 1)))
      if (first.getHash() == entry.getHash() && first == entry) {
        // if the entry is the header in the list
        val newFirst = first.getNext()
        Segment.connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
        Segment.nullifyAccessOrder(entry)
        entry.setNext(Entry.getNullEntry[K, V]())
        table.set((hash & (table.length() - 1)), newFirst)
        totalWeight -= entry.getValueReference().getWeight()
        return true
      }

      var e = first.getNext()
      var pre: Entry[K, V] = first
      while (e != null) {
        if (e.getHash() == hash && e == entry) {
          Segment.connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
          Segment.nullifyAccessOrder(entry)
          pre.setNext(entry.getNext())
          entry.setNext(Entry.getNullEntry[K, V]())
          totalWeight -= entry.getValueReference().getWeight()
          return true
        }

        pre = e
        e = e.getNext()
      }

      false
    } finally {
      writeLock.unlock()
    }
  }

  @inline private def recordRead(entry: Entry[K, V]): Unit = {
    // we need modify the accessQueue, so the write lock is required
    writeLock.lock()
    try {
      accessQueue.add(entry)
    } finally {
      writeLock.unlock()
    }
  }

  def contains(key: V, hash: Int): Boolean = {
    val first = table.get((hash & (table.length() - 1)))
  }

  def remove(key: K): Boolean = {
    lock.lock()
    try {

    } finally {
      lock.unlock()
    }
  }

  def clear(): Unit = {

  }
}

private[lxy] object Segment {
  @inline def connectAccessOrder[K, V](previous: Entry[K, V], next: Entry[K, V]): Unit = {
    previous.setNextInAccessQueue(next)
    next.setPreviousInAccessQueue(previous)
  }

  @inline def nullifyAccessOrder[K, V](entry: Entry[K, V]): Unit = {
    connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
    entry.setPreviousInAccessQueue(Entry.getNullEntry())
    entry.setNextInAccessQueue(Entry.getNullEntry())
  }
}

private[lxy] class AccessQueue[K: ClassTag, V: ClassTag] extends util.AbstractQueue[Entry[K, V]] {
  private var _size: Int = 0
  private val header = new AbstractEntry[K, V] {
    private var nextAccess: Entry[K, V] = this
    private var previousAccess: Entry[K, V] = this
    override def keyClassTag: ClassTag[K] = implicitly[ClassTag[K]]

    override def valueClassTag: ClassTag[V] = implicitly[ClassTag[V]]

    override def getNext(): Entry[K, V] = this

    override def getNextInAccessQueue(): Entry[K, V] = nextAccess

    override def setNextInAccessQueue(next: Entry[K, V]): Unit = {
      nextAccess = next
    }

    override def getPreviousInAccessQueue(): Entry[K, V] = previousAccess

    override def setPreviousInAccessQueue(previous: Entry[K, V]): Unit = {
      previousAccess = previous
    }
  }

  override def offer(e: Entry[K, V]): Boolean = {
    Segment.connectAccessOrder[K, V](e.getPreviousInAccessQueue(), e.getNextInAccessQueue())
    // put the entry in the tail
    // header.previous <-> header
    // header.previous <-> entry <-> header
    Segment.connectAccessOrder[K, V](header.getPreviousInAccessQueue(), e)
    Segment.connectAccessOrder[K, V](e, header)
    _size += 1
    true
  }

  override def poll(): Entry[K, V] = {
    if (isEmpty) {
      null
    } else {
      val entry = header.getNextInAccessQueue()
      remove(entry)
      entry
    }
  }

  override def peek(): Entry[K, V] = {
    if (!isEmpty) {
      header.getNextInAccessQueue()
    } else {
      null
    }
  }

  override def remove(o: scala.Any): Boolean = {
    if (contains(o)) {
      val entry = o.asInstanceOf[Entry[K, V]]
      Segment.connectAccessOrder[K, V](entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
      Segment.nullifyAccessOrder[K, V](entry)
      _size -= 1
      true
    } else {
      false
    }
  }

  override def contains(o: scala.Any): Boolean = {
    val entry = o.asInstanceOf[Entry[K, V]]
    entry.getNextInAccessQueue() != Entry.getNullEntry[K, V]()
  }

  override def isEmpty: Boolean = header != header.getNextInAccessQueue()

  override def iterator(): util.Iterator[Entry[K, V]] = {
    new util.Iterator[Entry[K, V]] {
      var entry = header.getNextInAccessQueue()
      override def hasNext: Boolean = entry != header

      override def next(): Entry[K, V] = {
        if (hasNext) {
          val previous = entry
          entry = entry.getNextInAccessQueue()
          previous
        } else {
          null
        }
      }
    }
  }

  override def size(): Int = _size

}

