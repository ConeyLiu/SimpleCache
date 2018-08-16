package com.lxy

import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantLock

import scala.reflect.ClassTag

//TODO: extend from AbstractMap?
class LRUCache[K, V](
    private final val concurrency: Int,
    private final val initialCapacity: Int,
    private final val maxWeight: Long,
    private final val weigher: Weigher[K, V],
    private final val defaultLoader: Loader[K, V],
    private final val satisfy: Satisfy[K, V],
    private final val removeListeners: Seq[RemoveListener[K, V]]) extends Cache[K, V]{

  private final val (segmentCount, shift, segmentInitialCapacity, weights) = init()
  private final val segments = new Array[Segment](segmentCount)
  (0 until segments.length).foreach{ i =>
    segments(i) =
      createSegment(segmentInitialCapacity, Integer.MAX_VALUE, weights(i), defaultLoader, satisfy, weigher)
  }
  private final val segmentShift = 32 - shift
  private final val segmentMask = segmentCount - 1

  private final val removalQueue = new LinkedBlockingQueue[Entry[K, V]]()

  private val removeTask = new Runnable {
    override def run(): Unit = {
      val entry = removalQueue.poll(500, TimeUnit.MILLISECONDS)
      if (entry != null) {
        removeListeners.foreach{ listener =>
          listener.onRemove(entry.getKey(), entry.getValue())
        }
      }
    }
  }

  // We used 2 thread to trigger the removeListeners, this may make the order of really removed is
  // different from the record order which in removalQueue.
  // TODO: make the size of the threadpool and remove order configurable.
  private final val removalExecutor = Executors.newScheduledThreadPool(2)
  removalExecutor.scheduleAtFixedRate(removeTask, 1, 1, TimeUnit.SECONDS)

  private def init(): (Int, Int, Int, Array[Long]) = {
    var segmentCount: Int = 1
    var segmentShift = 0
    while (segmentCount < concurrency) {
      segmentShift += 1
      segmentCount <<= 1
    }

    val segmentInitialCapacity = if (initialCapacity % segmentCount == 0) {
      initialCapacity / segmentCount
    } else {
      (initialCapacity / segmentCount) + 1
    }

    var segmentCapacity = 1
    while (segmentCapacity < segmentInitialCapacity) {
      segmentCapacity <<= 1
    }

    require(maxWeight > segmentCount, "Maximum weight is too small, please set more larger")
    val weightPerSegment = maxWeight / segmentCount
    val remainder = (maxWeight % segmentCount).toInt
    val weights = new Array[Long](segmentCount)
    (0 until segmentCount).foreach{ i =>
      weights(i) = weightPerSegment
    }

    (0 until remainder).foreach{ i =>
      weights(i) = weights(i) + 1
    }

    (segmentCount, segmentShift, segmentCapacity, weights)
  }

  private def createSegment(
      size: Int,
      maxSize: Int,
      maxWeight: Long,
      loader: Loader[K, V],
      satisfy: Satisfy[K, V],
      weigher: Weigher[K, V]): Segment = {
    new Segment(size, maxSize, maxWeight, loader, satisfy, weigher)
  }

  private def segmentFor(hash: Int): Segment = {
    segments((hash >>> segmentShift) & segmentMask)
  }

  override def get(key: K): Option[V] = {
    if (key == null) {
      return None
    }
    val hash = reHash(key)
    segmentFor(hash).get(key, hash)
  }

  override def get(key: K, loader: Loader[K, V]): Option[V] = {
    if (key == null) {
      return None
    }

    require(loader != null)
    val hash = reHash(key)
    segmentFor(hash).get(key, hash, loader)
  }

  override def contains(key: K): Boolean = {
    if (key == null) {
      return false
    }

    val hash = reHash(key)
    segmentFor(hash).contains(key, hash)
  }

  override def remove(key: K): Option[V] = {
    if (key == null) {
      return None
    }

    val hash = reHash(key)
    segmentFor(hash).remove(key, hash)
  }

  override protected def reHash(key: K): Int = {
    var h = key.hashCode()
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d
    h ^= (h >>> 10)
    h += (h << 3)
    h ^= (h >>> 6)
    h += (h << 2) + (h << 14)
    return h ^ (h >>> 16)
  }

  override protected def enqueueNotification(entry: Entry[K, V]): Unit = {
    removalQueue.offer(entry)
  }

  override protected def triggerListenerManual(entry: Entry[K, V]): Unit = {
    removeListeners.foreach{ listener =>
      listener.onRemove(entry.getKey(), entry.getValue())
    }
  }

  override def clear(): Unit = {
    segments.foreach(_.clear())
  }

  private class Segment(
      private final val size: Int,
      private final val maxSize: Int,
      private final val maxWeight: Long,
      private final val defaultLoader: Loader[K, V],
      private final val satisfy: Satisfy[K, V],
      private final val weigher: Weigher[K, V]){
    private val lock = new ReentrantLock()
    @volatile private var totalWeight: Long = 0L
    @volatile private var table = new AtomicReferenceArray[Entry[K, V]](size)
    @volatile private var count: Int = 0
    @volatile private var threshold = size * 3 / 4
    private val recentQueue = new ConcurrentLinkedQueue[Entry[K, V]]()
    @volatile private var accessQueue = new AccessQueue[K, V]()

    def get(key: K, hash: Int): Option[V] = {
      get(key, hash, defaultLoader)
    }

    def get(key: K, hash: Int, loader: Loader[K, V]): Option[V] = {
      if (count != 0) {
        val first = table.get((hash & (table.length() - 1)))
        var entry: Entry[K, V] = first
        while (entry != null && (entry.getHash() != hash || entry.getKey() != key)) {
          entry = entry.getNext()
        }

        if (entry != null) {
          val value = entry.getValue()
          recordRead(entry)
          Some(value)
        } else {
          Option(put(key, hash, first, defaultLoader))
        }
      } else {
        Option(put(key, hash, null, defaultLoader))
      }

    }

    private def put(key: K, hash: Int, head: Entry[K, V], loader: Loader[K, V]): V = {
      lock.lock()
      try {
        prepareWrite()

        var entry: Entry[K, V] = head
        while (entry != null && (entry.getHash() != hash || entry.getKey() != key)) {
          entry = entry.getNext()
        }

        // if another thread already load it, just return it
        if (entry != null) {
          val value = entry.getValue()
          recordReadWithLock(entry)
          return value
        }

        if (count > threshold) {
          expand()
        }

        val loadingStart = System.nanoTime()
        val value = loader.load(key)
        val loadingTime = System.nanoTime() - loadingStart
        val weight = weigher.weight(key, value)
        // First, we need to satisfy the weight condition's check
        if (weight > (maxWeight - totalWeight)) {
          // if there aren't enough space, evict it.
          evict(maxWeight - weight)
        }

        // Second, we need to satisfy the `Satisfy`'s check
        while (!satisfy.check(key, value, weight)) {
          val evicted = evict()
          triggerListenerManual(evicted)
        }

        totalWeight += weight
        count += 1
        val newEntry = Entry(key, hash, value, weight, head)
        table.set((hash & (table.length() - 1)), newEntry)
        recordReadWithLock(newEntry)
        value
      } finally {
        lock.unlock()
      }
    }

    private def prepareWrite(): Unit = {
      while (!recentQueue.isEmpty) {
        accessQueue.add(recentQueue.poll())
      }
    }

    private def evict(): Entry[K, V] = {
      val evictedEntry = accessQueue.peek()
      removeEntryFromChain(evictedEntry)
      evictedEntry
    }

    private def evict(max: Long): Unit = {
      while (totalWeight > max) {
        evict()
      }
    }

    private def enqueueNotification(entry: Entry[K, V]): Unit = {
      totalWeight -= entry.getWeight
      count -= 1
      LRUCache.this.enqueueNotification(entry)
    }

    /**
      * Remove a entry from the entry chain. we need guarantee that it doesn't impact the read when
      * removing the entry, so we need copy those entry which from head to target.
      * @param entry The entry need to be removed
      */
    private def removeEntryFromChain(entry: Entry[K, V]): Unit = {
      lock.lock()
      try {
        enqueueNotification(entry)
        // Modify accessQueue need be guard by lock, so we should remove the entry in accessQueue at here
        accessQueue.remove(entry)
        val index = entry.getHash() & (table.length() - 1)
        val head = table.get(index)
        var e = head
        var newHead = entry.getNext()
        while (e != entry) {
          newHead = Entry.copy(e, newHead)
          e = e.getNext()
        }
        table.set(index, newHead)
      } finally {
        lock.unlock()
      }
    }

    @inline private def recordRead(entry: Entry[K, V]): Unit = {
      recentQueue.add(entry)
    }

    @inline private def recordReadWithLock(entry: Entry[K, V]): Unit = {
      accessQueue.add(entry)
    }

    private def expand(): Unit = {
      lock.lock()
      try {
        val oldSize = table.length()
        if (oldSize >= maxSize) {
          return
        }

        val newSize = oldSize << 1
        val newTable = new AtomicReferenceArray[Entry[K, V]](newSize)
        threshold = newSize * 3 / 4
        val newMask = newSize -1
        (0 until newSize).foreach { i =>
          val head = table.get(i)
          val newHeadIndex = head.getHash() & newMask
          if (head != null) {
            if (head.getNext() == null) {
              newTable.set(newHeadIndex, head)
            } else {
              var tailNewIndex = newHeadIndex
              var tailEntry = head
              var e = head
              while (e != null) {
                val newIndex = e.getHash() & newMask
                if (newIndex != tailNewIndex) {
                  tailNewIndex = newIndex
                  tailEntry = e
                }

                e = e.getNext()
              }

              newTable.set(tailNewIndex, tailEntry)
              e = head
              while (e != tailEntry) {
                val newIndex = e.getHash() & newMask
                val newNext = newTable.get(newIndex)
                val newHead = Entry.copy(e, newNext)
                newTable.set(newIndex, newHead)
              }
            }
          }
        }

        table = newTable

      } finally {
        lock.unlock()
      }
    }


    def contains(key: K, hash: Int): Boolean = {
      if (count != 0) {
        var entry = table.get((hash & (table.length() - 1)))
        while (entry != null) {
          if (entry.getHash() == hash && entry.getKey() == key) {
            return true
          }

          entry = entry.getNext()
        }

        false
      } else {
        false
      }
    }

    def remove(key: K, hash: Int): Option[V] = {
      var head = table.get(hash & (table.length() - 1))
      while (head != null) {
        if (head.getHash() == hash && head.getKey() == key) {
          removeEntryFromChain(head)
          return Option(head.getValue())
        }

        head = head.getNext()
      }

      None
    }

    def clear(): Unit = {
      if (count != 0) {
        lock.lock()
        prepareWrite()
        try {
          (0 until table.length()).foreach { i =>
            var head = table.get(i)
            while (head != null) {
              enqueueNotification(head)
              head = head.getNext()
            }
          }

          (0 until table.length()).foreach { i =>
            table.set(i, null)
          }

          accessQueue.clear()
          recentQueue.clear()
          count = 0

        } finally {
          lock.unlock()
        }
      }
    }
  }
}

private[lxy] object LRUCache {
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

/**
  * A dequeue used to record the access order of key. This is not threadsafe, modify should be under lock.
  */
private[lxy] class AccessQueue[K, V] extends util.AbstractQueue[Entry[K, V]] {
  private var _size: Int = 0
  private val header = new AbstractEntry[K, V] {
    private var nextAccess: Entry[K, V] = this
    private var previousAccess: Entry[K, V] = this

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
    LRUCache.connectAccessOrder[K, V](e.getPreviousInAccessQueue(), e.getNextInAccessQueue())
    // put the entry in the tail
    // header.previous <-> header
    // header.previous <-> entry <-> header
    LRUCache.connectAccessOrder[K, V](header.getPreviousInAccessQueue(), e)
    LRUCache.connectAccessOrder[K, V](e, header)
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
      LRUCache.connectAccessOrder[K, V](entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
      LRUCache.nullifyAccessOrder[K, V](entry)
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

  override def isEmpty: Boolean = header == header.getNextInAccessQueue()

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

  override def clear(): Unit = {
    var entry = header.getNextInAccessQueue()
    while (entry != header) {
      val next = entry.getNextInAccessQueue()
      LRUCache.nullifyAccessOrder(entry)
      entry = next
    }

    header.setPreviousInAccessQueue(header)
    header.setNextInAccessQueue(header)
  }
}

