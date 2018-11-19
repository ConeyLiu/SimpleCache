package com.lxy

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}

import net.jcip.annotations.{GuardedBy, NotThreadSafe, ThreadSafe}

// TODO: extend from AbstractMap?
@ThreadSafe
class LRUCache[K, V, R](
    private final val concurrency: Int,
    private final val initialCapacity: Int,
    private final val maxWeight: Long,
    private final val weigher: Weigher[K, V],
    private final val defaultLoader: Loader[K, V],
    private final val cacheHandler: CacheHandler[K, V, R],
    private final val removeListeners: Seq[RemoveListener[K, R]],
    private final val forceRemoveListeners: Seq[RemoveListener[K, R]])
  extends Cache[K, V, R]{

  private final val cacheStatusCounter = CacheStatusCounter()
  private final val (segmentCount, shift, segmentInitialCapacity, weights) = init()
  private final val segments = new Array[Segment](segmentCount)
  segments.indices.foreach{ i =>
    segments(i) =
      createSegment(segmentInitialCapacity, Integer.MAX_VALUE, weights(i))
  }
  private final val segmentShift = 32 - shift
  private final val segmentMask = segmentCount - 1
  private val needEvict = new AtomicInteger(0)

  //  private final val removalQueue = new ConcurrentLinkedQueue[Entry[K, R]]()
  //
  //  private val removeTask = new Runnable {
  //    override def run(): Unit = {
  //      val entry = removalQueue.poll()
  //      if (entry != null) {
  //        removeListeners.foreach{ listener =>
  //          listener.onRemove(entry.getKey(), entry.getValue())
  //        }
  //      }
  //    }
  //  }
  //
  //  // We used 8 thread to trigger the removeListeners, this may make the order of really removed is
  //  // different from the record order which in removalQueue.
  //  // TODO: make the size of the threadpool and remove order configurable.
  //  private final val removalExecutor = Executors.newScheduledThreadPool(8)
  //  removalExecutor.scheduleAtFixedRate(removeTask, 0, 200, TimeUnit.MILLISECONDS)

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
    new Segment(size, maxSize, maxWeight, cacheStatusCounter,
      removeListeners, forceRemoveListeners)
  }

  private def segmentFor(hash: Int): Segment = {
    segments((hash >>> segmentShift) & segmentMask)
  }

  override def get(key: K): Option[R] = {
    get(key, null)
  }

  override def get(key: K, loader: Loader[K, V]): Option[R] = {
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


  override def getKeys(): Set[K] = new Iterator[K] {
    var index = 0
    var cur: Iterator[Entry[K, R]] = null

    override def hasNext: Boolean = {
      if (cur == null || !cur.hasNext) {
        while ((cur == null || !cur.hasNext) && index < LRUCache.this.segmentCount) {
          cur = LRUCache.this.segments(index).iterator()
          index += 1
        }
      }

      cur != null && cur.hasNext
    }

    override def next(): K = {
      if (hasNext) {
        cur.next().getKey()
      } else {
        throw new IndexOutOfBoundsException
      }
    }
  }.toSet

  override def contains(key: K): Boolean = {
    if (key == null) {
      return false
    }

    val hash = reHash(key)
    segmentFor(hash).contains(key, hash)
  }

  override def remove(key: K): Option[R] = {
    if (key == null) {
      return None
    }

    val hash = reHash(key)
    segmentFor(hash).remove(key, hash)
  }

  override def size(): Long = {
    segments.foldLeft(0L)((pre, cur) => pre + cur.size)
  }

  private def sumOfWeight(): Long = {
    segments.foldLeft(0L)((pre, cur) => pre + cur.sumOfWeight())
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

  override def clear(): Unit = {
    segments.foreach(_.clear())
  }

  private class Segment(
      private final val initialSize: Int,
      private final val maxSize: Int,
      private final val maxWeight: Long,
      private final val cacheStatusCounter: CacheStatusCounter,
      private final val removeListeners: Seq[RemoveListener[K, R]],
      private final val forceRemoveListeners: Seq[RemoveListener[K, R]]) {
    @volatile private var totalWeight: Long = 0L
    @volatile private var table = new AtomicReferenceArray[Entry[K, R]](initialSize)
    @GuardedBy("this") @volatile private var count: Int = 0
    @volatile private var threshold = initialSize * 3 / 4
    @volatile private var accessQueue = new AccessQueue[K, R]()

    private val recentQueue = new LinkedBlockingQueue[Entry[K, R]]()

    def get(key: K, hash: Int): Option[R] = {
      get(key, hash, defaultLoader)
    }

    def get(key: K, hash: Int, loader: Loader[K, V]): Option[R] = {
      if (count != 0) {
        val first = table.get(hash & (table.length() - 1))
        var entry: Entry[K, R] = first
        while (entry != null) {
          if (entry.getHash() == hash && entry.getKey() == key && entry.isValid()) {
            val value = entry.getValue()
            recordRead(entry)
            return Some(value)
          }
          entry = entry.getNext()
        }

        Option(put(key, hash, loader))
      } else {
        Option(put(key, hash, loader))
      }
    }

    @GuardedBy("this")
    private def put(key: K, hash: Int, loader: Loader[K, V]): R = {
      this.synchronized {

        // force evict firstly
        forceEvict()

        prepareWrite()

        val head = table.get(hash & (table.length() - 1))
        var entry: Entry[K, R] = head
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
        val weight = weigher.weight(key, value)

        require(weight <= maxWeight, s"The weight: ${weight} of entry(key: ${key}, value: " +
          s"${value}) should be less than or equal to maxWeight: ${maxWeight}")

        // First, we need to satisfy the weight condition's check
        if (weight > (maxWeight - totalWeight)) {
          // if there aren't enough space, evict it.
          evict(maxWeight - weight)
        }

        var address = cacheHandler.allocate(key, value)
        var resetEvictCount = false
        if (address == 0) {
          LRUCache.this.needEvict.incrementAndGet()
          resetEvictCount = true
          val limit = count / 2
          var entry: Entry[K, R] = null
          while (address == 0 && count > limit) {
            entry = accessQueue.peek()
            assert(entry != Entry.getNullEntry(),
              s"AccessQueue shouldn't contain null entry, ${entry.isValid()}")
            removeEntryFromChain(entry, forceRemoveListeners)
            address = cacheHandler.allocate(key, value)
          }
        }

        // We sleep 5 seconds for other segments to evict
        if (address == 0) {
          val end = System.currentTimeMillis() + 5000
          while (address == 0 && System.currentTimeMillis() < end) {
            try {
              Thread.sleep(end - System.currentTimeMillis())
              address = cacheHandler.allocate(key, value)
            } catch {
              case _: InterruptedException =>
                Thread.interrupted()
                address = cacheHandler.allocate(key, value)
            }
          }
        }

        if (address == 0) {
          address = cacheHandler.allocate(key, value)
        }

        if (resetEvictCount) {
          // we need to reset it back even we don't get enough memory.
          LRUCache.this.needEvict.addAndGet(-1)
        }

        require(address != 0, s"THREAD-${Thread.currentThread().getId}. There aren't enough " +
          s"memory after eviction, please consider increase the memory limit. Key: ${key}, " +
          s"value: ${value}.")
        // TODO: we need to support cache data asynchronously
        val result = cacheHandler.cache(key, value, address)
        if (result.isEmpty) {
          throw new RuntimeException(s"Cache failed: key: ${key}, value: ${value}, address: " +
            s"${address}")
        }

        // add the cache time together with the loading time.
        val loadingTime = System.nanoTime() - loadingStart
        cacheStatusCounter.totalLoadTimeCounter.addAndGet(loadingTime / 1000)
        cacheStatusCounter.loadCounter.incrementAndGet()
        cacheStatusCounter.missCounter.incrementAndGet()

        // a little trick
        val actualWeight = weigher.weight(key, result.get.asInstanceOf[V])
        totalWeight += actualWeight
        count += 1
        // the head maybe removed, so we need get the head from the table
        val updatedHead = table.get(hash & (table.length() - 1))
        val newEntry = Entry(key, hash, result.get, actualWeight, updatedHead)
        table.set(hash & (table.length() - 1), newEntry)
        recordReadWithLock(newEntry)

        if (count >= threshold) {
          expand()
        }

        result.get
      }
    }


    @GuardedBy("this")
    private def prepareWrite(): Unit = {
      while (!recentQueue.isEmpty) {
        accessQueue.offer(recentQueue.poll())
      }
    }

    /**
     * We need force evict some entry when we can't request enough memory. And also we only
     * evict those segments that occupied more than total/segmentCount. And evict at mostly
     * the 2/3 of the entries.
     */
    @GuardedBy("this")
    private def forceEvict(): Unit = {
      val limit = count / 2
      val segmentCount = LRUCache.this.segmentCount
      val weightLimit = LRUCache.this.sumOfWeight() / segmentCount
      while (LRUCache.this.needEvict.get() > 0 &&
        totalWeight > weightLimit &&
        count > limit) {
        val entry = accessQueue.peek()
        removeEntryFromChain(entry, forceRemoveListeners)
      }
    }

    @GuardedBy("this")
    private def evict(): Entry[K, R] = {
      val entry = accessQueue.peek()
      require(entry != Entry.getNullEntry(), "Null entry shouldn't be existed in access queue.")
      if (entry != null) {
        removeEntryFromChain(entry, removeListeners)
      }
      entry
    }

    /**
     * Evict the total weight to max weight.
     */
    @GuardedBy("this")
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
     */
    @GuardedBy("this")
    private def removeEntryFromChain(
        entry: Entry[K, R],
        listeners: Seq[RemoveListener[K, R]]): Unit = {
      this.synchronized {
        require(entry != null || entry != Entry.getNullEntry(),
          "Can't remove null entry from chain")
        // mark the entry as invalid
        entry.markAsInvalid()
        listeners.foreach(_.onRemove(entry.getKey(), entry.getValue()))
        // Modify accessQueue need be guard by lock, so we should remove the entry in accessQueue
        // at here
        accessQueue.remove(entry)
        val index = entry.getHash() & (table.length() - 1)
        val head = table.get(index)
        var e = head
        var newHead: Entry[K, R] = entry.getNext()
        while (e != entry) {
          newHead = copyEntry(e, newHead)
          e = e.getNext()
        }

        table.set(index, newHead)
        totalWeight -= entry.getWeight
        count -= 1
        cacheStatusCounter.evictionCounter.incrementAndGet()
      }
    }

    @inline private def recordRead(entry: Entry[K, R]): Unit = {
      recentQueue.add(entry)
      cacheStatusCounter.hitCounter.incrementAndGet()
    }

    @GuardedBy("this")
    @inline private def recordReadWithLock(entry: Entry[K, R]): Unit = {
      accessQueue.offer(entry)
    }

    @GuardedBy("this")
    private def copyEntry(original: Entry[K, R], next: Entry[K, R]): Entry[K, R] = {
      require(original != null && original != Entry.getNullEntry(),
        "The original entry can't be null")
      val entry = new ConcreteEntry[K, R](original.getKey(), original.getHash(),
        original.getValue(), original.getWeight, next)
      LRUCache.connectAccessOrder(original.getPreviousInAccessQueue(), entry)
      LRUCache.connectAccessOrder(entry, original.getNextInAccessQueue())
      LRUCache.nullifyAccessOrder(original)
      // mark as invalid and then it won't be add to accessQueue
      original.markAsInvalid()
      entry
    }

    @GuardedBy("this")
    private def expand(): Unit = {
      val oldSize = table.length()
      if (oldSize >= maxSize) {
        return
      }

      val newSize = oldSize << 1
      val newTable = new AtomicReferenceArray[Entry[K, R]](newSize)
      threshold = newTable.length() * 3 / 4
      val newMask = newTable.length() - 1
      (0 until oldSize).foreach { i =>
        val head = table.get(i)
        if (head != null) {
          val newHeadIndex = head.getHash() & newMask
          if (head.getNext() == null) {
            newTable.set(newHeadIndex, head)
          } else {
            var tailNewIndex = newHeadIndex
            var tailEntry = head
            var e = head.getNext()
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
              val newHead = copyEntry(e, newNext)
              newTable.set(newIndex, newHead)
              e = e.getNext()
            }
          }
        }
      }

      table = newTable
    }

    def contains(key: K, hash: Int): Boolean = {
      if (count != 0) {
        var entry = table.get(hash & (table.length() - 1))
        while (entry != null) {
          if (entry.getHash() == hash && entry.getKey() == key && entry.isValid()) {
            return true
          }

          entry = entry.getNext()
        }

        false
      } else {
        false
      }
    }

    def remove(key: K, hash: Int): Option[R] = {
      if (count != 0) {
        var head = table.get(hash & (table.length() - 1))
        while (head != null) {
          if (head.getHash() == hash && head.getKey() == key && head.isValid()) {
            removeEntryFromChain(head, removeListeners)
            return Option(head.getValue())
          }

          head = head.getNext()
        }
      }

      None
    }

    def iterator(): Iterator[Entry[K, R]] = new Iterator[Entry[K, R]] {
      var index = 0
      var entry: Entry[K, R] = null
      def t: AtomicReferenceArray[Entry[K, R]] = table

      override def hasNext: Boolean = {
        if (entry == null && index < t.length()) {
          while (entry == null && index < t.length()) {
            entry = t.get(index)
            index += 1
          }
        }

        entry != null && entry != Entry.getNullEntry()
      }

      override def next(): Entry[K, R] = {
        if (hasNext) {
          val cur = entry
          entry = entry.getNext()
          cur
        } else {
          throw new IndexOutOfBoundsException
        }
      }
    }

    def size(): Int = count

    def sumOfWeight(): Long = totalWeight

    @GuardedBy("this")
    def clear(): Unit = {
      if (count == 0) {
        return
      }

      this.synchronized {
        prepareWrite()
        try {
          (0 until table.length()).foreach { i =>
            var head = table.get(i)
            while (head != null) {
              removeListeners.foreach(_.onRemove(head.getKey(), head.getValue()))
              head = head.getNext()
            }
          }

          (0 until table.length()).foreach { i =>
            table.set(i, null)
          }

          accessQueue.clear()
          recentQueue.clear()
          count = 0
        }
      }
    }
  }
}

object LRUCache {
  @inline def connectAccessOrder[K, R](previous: Entry[K, R], next: Entry[K, R]): Unit = {
    previous.setNextInAccessQueue(next)
    next.setPreviousInAccessQueue(previous)
  }

  @inline def nullifyAccessOrder[K, R](entry: Entry[K, R]): Unit = {
    entry.setPreviousInAccessQueue(Entry.getNullEntry())
    entry.setNextInAccessQueue(Entry.getNullEntry())
  }
}

/**
 * A queue used to record the access order of key. This is not threadsafe, modification should
 * be under lock.
 */
@NotThreadSafe
private[lxy] class AccessQueue[K, R] extends util.AbstractQueue[Entry[K, R]] {
  private var _size: Int = 0
  private val header: Entry[K, R] = new AbstractEntry[K, R] {
    @volatile private var nextAccess: Entry[K, R] = this
    @volatile private var previousAccess: Entry[K, R] = this

    override def getNext(): Entry[K, R] = this

    override def getNextInAccessQueue(): Entry[K, R] = nextAccess

    override def setNextInAccessQueue(next: Entry[K, R]): Unit = {
      nextAccess = next
    }

    override def getPreviousInAccessQueue(): Entry[K, R] = previousAccess

    override def setPreviousInAccessQueue(previous: Entry[K, R]): Unit = {
      previousAccess = previous
    }
  }

  override def offer(e: Entry[K, R]): Boolean = {
    if (e == null || e == Entry.getNullEntry() || !e.isValid()) {
      return false
    }

    if (!contains(e)) {
      _size += 1
    }

    LRUCache.connectAccessOrder(e.getPreviousInAccessQueue(), e.getNextInAccessQueue())
    // put the entry in the tail
    // header.previous <-> header
    // header.previous <-> entry <-> header
    LRUCache.connectAccessOrder(header.getPreviousInAccessQueue(), e)
    LRUCache.connectAccessOrder(e, header)
    true
  }

  override def poll(): Entry[K, R] = {
    if (isEmpty) {
      null
    } else {
      val entry = header.getNextInAccessQueue()
      remove(entry)
      entry
    }
  }

  override def peek(): Entry[K, R] = {
    if (!isEmpty) {
      header.getNextInAccessQueue()
    } else {
      null
    }
  }

  override def remove(o: scala.Any): Boolean = {
    if (contains(o)) {
      val entry = o.asInstanceOf[Entry[K, R]]
      LRUCache.connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
      LRUCache.nullifyAccessOrder(entry)
      _size -= 1
      true
    } else {
      false
    }
  }

  override def contains(o: scala.Any): Boolean = {
    val entry = o.asInstanceOf[Entry[K, R]]
    entry.getNextInAccessQueue() != Entry.getNullEntry[K, R]()
  }

  override def isEmpty: Boolean = header == header.getNextInAccessQueue()

  override def iterator(): util.Iterator[Entry[K, R]] = {
    new util.Iterator[Entry[K, R]] {
      var entry: Entry[K, R] = header.getNextInAccessQueue()
      override def hasNext: Boolean = entry != header

      override def next(): Entry[K, R] = {
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
      LRUCache.connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue())
      LRUCache.nullifyAccessOrder(entry)
      entry = next
    }

    header.setPreviousInAccessQueue(header)
    header.setNextInAccessQueue(header)
    _size = 0
  }
}

