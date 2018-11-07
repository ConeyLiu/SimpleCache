package com.lxy

import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.{ReadWriteLock, ReentrantLock}

import net.jcip.annotations.{GuardedBy, NotThreadSafe, ThreadSafe}

//TODO: extend from AbstractMap?
@ThreadSafe
class LRUCache[K, V](
    private final val concurrency: Int,
    private final val initialCapacity: Int,
    private final val maxWeight: Long,
    private final val weigher: Weigher[K, V],
    private final val defaultLoader: Loader[K, V],
    private final val cacheHandler: CacheHandler[K, V],
    private final val removeListeners: Seq[RemoveListener[K, V]],
    private final val lockManagement: LockManagement[K]) extends Cache[K, V]{

  private final val cacheStatusCounter = CacheStatusCounter()
  private final val (segmentCount, shift, segmentInitialCapacity, weights) = init()
  private final val segments = new Array[Segment](segmentCount)
  segments.indices.foreach{ i =>
    segments(i) =
      createSegment(segmentInitialCapacity, Integer.MAX_VALUE, weights(i))
  }
  private final val segmentShift = 32 - shift
  private final val segmentMask = segmentCount - 1

  private final val removalQueue = new LinkedBlockingQueue[Entry[K, V]]()

  private val removeTask = new Runnable {
    override def run(): Unit = {
      val entry = removalQueue.poll()
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
  removalExecutor.scheduleAtFixedRate(removeTask, 0, 200, TimeUnit.MILLISECONDS)

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
      maxWeight: Long): Segment = {
    new Segment(size, maxSize, maxWeight, cacheStatusCounter)
  }

  private def segmentFor(hash: Int): Segment = {
    segments((hash >>> segmentShift) & segmentMask)
  }

  override def get(key: K): Option[V] = {
    get(key, null)
  }

  override def get(key: K, loader: Loader[K, V]): Option[V] = {
    if (key == null) {
      return None
    }

    val hash = reHash(key)
    if (null == loader) {
      segmentFor(hash).get(key, hash)
    } else {
      segmentFor(hash).get(key, hash, loader)
    }
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

  override def size(): Long = {
    segments.foldLeft(0L)((pre, cur) => pre + cur.size)
  }

  override def status(): CacheStatus = {
    cacheStatusCounter.snapshot()
  }

  private def reHash(key: K): Int = {
    var h = key.hashCode()
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d
    h ^= (h >>> 10)
    h += (h << 3)
    h ^= (h >>> 6)
    h += (h << 2) + (h << 14)
    h ^ (h >>> 16)
  }

  private def enqueueNotification(entry: Entry[K, V]): Unit = {
    removalQueue.offer(entry)
  }

  private def removalQueueIsEmpty(): Boolean = removalQueue.isEmpty

  /**
   * Force evict the entry by current thread, we should first get the lock of the entry.
   */
  private[LRUCache] def forceEviction(entry: Entry[K, V]): Unit = {
    removeListeners.foreach{ listener =>
      listener.onRemove(entry.getKey(), entry.getValue())
    }
  }

  override def clear(): Unit = {
    segments.foreach(_.clear())
  }

  private class Segment(
      private final val initialSize: Int,
      private final val maxSize: Int,
      private final val maxWeight: Long,
      private final val cacheStatusCounter: CacheStatusCounter){
    @volatile private var totalWeight: Long = 0L
    @volatile private var table = new AtomicReferenceArray[Entry[K, V]](initialSize)
    @GuardedBy("lock") @volatile private var count: Int = 0
    @volatile private var threshold = initialSize * 3 / 4
    @volatile private var accessQueue = new AccessQueue[K, V]()

    private val lock = new ReentrantLock()
    private val recentQueue = new ConcurrentLinkedQueue[Entry[K, V]]()

    def get(key: K, hash: Int): Option[V] = {
      get(key, hash, defaultLoader)
    }

    def get(key: K, hash: Int, loader: Loader[K, V]): Option[V] = {
      if (count != 0) {
        val first = table.get(hash & (table.length() - 1))
        var entry: Entry[K, V] = first
        while (entry != null && (entry.getHash() != hash || entry.getKey() != key)) {
          entry = entry.getNext()
        }

        if (entry != null) {
          val value = entry.getValue()
          recordRead(entry)
          Some(value)
        } else {
          Option(put(key, hash, loader))
        }
      } else {
        Option(put(key, hash, loader))
      }
    }

    @GuardedBy("lock")
    private def put(key: K, hash: Int, loader: Loader[K, V]): V = {
      lock.lock()
      try {
        prepareWrite()

        val head = table.get(hash & (table.length() - 1))
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

        val loadingStart = System.nanoTime()
        val value = loader.load(key)
        val loadingTime = System.nanoTime() - loadingStart
        val weight = weigher.weight(key, value)

        cacheStatusCounter.totalLoadTimeCounter.addAndGet(loadingTime / 1000)
        cacheStatusCounter.loadCounter.incrementAndGet()
        cacheStatusCounter.missCounter.incrementAndGet()

        require(weight <= maxWeight, s"The weight: ${weight} of entry(key: ${key}, value: ${value}) " +
          s"should be less than or equal to maxWeight: ${maxWeight}")

        // First, we need to satisfy the weight condition's check
        if (weight > (maxWeight - totalWeight)) {
          // if there aren't enough space, evict it.
          evict(maxWeight - weight)
        }

        var address = cacheHandler.allocate(key, value)
        if (address == 0) {
          // there maybe memory fragmentation

          // if the removal queue is not empty, we need wait it do some post process for 1000 ms.
          // TODO: make the wait time configurable
          if (!LRUCache.this.removalQueueIsEmpty()) {
            val end = System.currentTimeMillis() + 1000
            while (address == 0 && System.currentTimeMillis() < end) {
              val sleepTime = end - System.currentTimeMillis()
              if (sleepTime > 0) {
                try {
                  Thread.sleep(sleepTime)
                  address = cacheHandler.allocate(key, value)
                } catch {
                  case _: InterruptedException =>
                    address = cacheHandler.allocate(key, value)
                    Thread.interrupted()
                }
              }
            }
          }

          // if still can't request enough memory, we need to force evict some entry
          if (address == 0) {
            // here, we can't call evict method directly, because the evicted entry maybe used by some task.
            // So, we need hold the exclusive lock for the given entry.
            val iterator = accessQueue.iterator()
            var entry: Entry[K, V] = null
            var lock: ReadWriteLock = null
            while (address == 0 && iterator.hasNext) {
              entry = iterator.next()
              lock = lockManagement.getLock(entry.getKey())
              if (lock.writeLock().tryLock(200, TimeUnit.MILLISECONDS)) {
                try {
                  removeEntryFromChain(entry, false)
                  LRUCache.this.forceEviction(entry)
                  address = cacheHandler.allocate(key, value)
                } finally {
                  lock.writeLock().unlock()
                  lockManagement.removeLock(entry.getKey())
                }
              }
            }
          }
        }

        require(address != 0, "There aren't enough memory after eviction, please consider increase the memory limit")
        // TODO: we need to support cache data asynchronously
        val result = cacheHandler.cache(key, value, address)
        if (!result) {
          throw new RuntimeException(s"Cache failed: key: ${key}, value: ${value}, address: ${address}")
        }
        totalWeight += weight
        count += 1
        // the head maybe removed, so we need get the head from the table
        val updatedHead = table.get(hash & (table.length() - 1))
        val newEntry = Entry(key, hash, value, weight, updatedHead)
        table.set(hash & (table.length() - 1), newEntry)
        recordReadWithLock(newEntry)

        if (count >= threshold) {
          expand()
        }

        value
      } finally {
        lock.unlock()
      }
    }

    @GuardedBy("lock")
    private def prepareWrite(): Unit = {
      while (!recentQueue.isEmpty) {
        accessQueue.add(recentQueue.poll())
      }
    }

    @GuardedBy("lock")
    private def evict(): Entry[K, V] = {
      val evictedEntry = accessQueue.peek()
      if (evictedEntry != null) {
        removeEntryFromChain(evictedEntry, true)
        cacheStatusCounter.evictionCounter.incrementAndGet()
      }
      evictedEntry
    }

    /**
     * Evict the total weight to max weight.
     */
    @GuardedBy("lock")
    private def evict(max: Long): Unit = {
      var continue = true
      while (continue && totalWeight > max) {
        continue = evict() != null
      }
    }

    /**
     * Remove an entry from the entry chain. we need guarantee that it doesn't impact the read when
     * removing the entry, so we need copy those entry which from head to target.
     * @param entry The entry need to be removed
     * @param enqueue whether enqueue the removal queue
     */
    @GuardedBy("lock")
    private def removeEntryFromChain(entry: Entry[K, V], enqueue: Boolean): Unit = {
      lock.lock()
      try {
        require(entry != null, "Can't remove null entry from chain")
        if (enqueue) {
          LRUCache.this.enqueueNotification(entry)
        }

        // Modify accessQueue need be guard by lock, so we should remove the entry in accessQueue at here
        accessQueue.remove(entry)
        val index = entry.getHash() & (table.length() - 1)
        val head = table.get(index)
        var e = head
        var newHead: Entry[K, V] = null
        while (e != null) {
          if (e != entry) {
            newHead = Entry.copy(e, newHead)
          }
          e = e.getNext()
        }

        table.set(index, newHead)
        totalWeight -= entry.getWeight
        count -= 1
      } finally {
        lock.unlock()
      }
    }

    @inline private def recordRead(entry: Entry[K, V]): Unit = {
      recentQueue.add(entry)
      cacheStatusCounter.hitCounter.incrementAndGet()
    }

    @GuardedBy("lock")
    @inline private def recordReadWithLock(entry: Entry[K, V]): Unit = {
      accessQueue.add(entry)
    }

    @GuardedBy("lock")
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
        (0 until oldSize).foreach { i =>
          val head = table.get(i)
          if (head != null) {
            val newHeadIndex = head.getHash() & newMask
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
                e = e.getNext()
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
        var entry = table.get(hash & (table.length() - 1))
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
      if (count != 0) {
        var head = table.get(hash & (table.length() - 1))
        while (head != null) {
          if (head.getHash() == hash && head.getKey() == key) {
            removeEntryFromChain(head, true)
            return Option(head.getValue())
          }

          head = head.getNext()
        }
      }

      None
    }

    def size(): Int = {count}

    @GuardedBy("lock")
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

/**
 * A queue used to record the access order of key. This is not threadsafe, modification should be under lock.
 */
@NotThreadSafe
private[lxy] class AccessQueue[K, V] extends util.AbstractQueue[Entry[K, V]] {
  private var _size: Int = 0
  private val header: Entry[K, V] = new AbstractEntry[K, V] {
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
    if (!contains(e)) {
      _size += 1
    }

    connectAccessOrder(e.getPreviousInAccessQueue(), e.getNextInAccessQueue())
    // put the entry in the tail
    // header.previous <-> header
    // header.previous <-> entry <-> header
    connectAccessOrder(header.getPreviousInAccessQueue(), e)
    connectAccessOrder(e, header)
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
      connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
      nullifyAccessOrder(entry)
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
      var entry: Entry[K, V] = header.getNextInAccessQueue()
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
      nullifyAccessOrder(entry)
      entry = next
    }

    header.setPreviousInAccessQueue(header)
    header.setNextInAccessQueue(header)
    _size = 0
  }

  @inline def connectAccessOrder(previous: Entry[K, V], next: Entry[K, V]): Unit = {
    previous.setNextInAccessQueue(next)
    next.setPreviousInAccessQueue(previous)
  }

  @inline def nullifyAccessOrder(entry: Entry[K, V]): Unit = {
    connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
    entry.setPreviousInAccessQueue(Entry.getNullEntry())
    entry.setNextInAccessQueue(Entry.getNullEntry())
  }
}

