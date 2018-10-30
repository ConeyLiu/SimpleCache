package com.lxy

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AccessQueueSuite extends FunSuite with BeforeAndAfterAll{

  var data: Seq[Entry[Int, Int]] = null

  override def beforeAll(): Unit = {
    data = (0 until 10).map {i =>
      Entry(i, reHash(i), i, 1, null)
    }
  }

  override def afterAll(): Unit = {
    data = null
  }

  test("Normally test") {
    val queue = new AccessQueue[Int, Int]
    data.foreach(queue.offer(_))
    val array = queue.toArray()
    assert(data === array)

    assert(queue.size() === 10)
    val entry = queue.peek()
    assert(entry.getKey() === 0, "Peek entry should be the head entry")
    val polled = queue.poll()
    assert(entry === polled, "Poll entry should be the head entry")
    assert(queue.peek().getKey() === 1, "After polled, peek entry should be the entry next to previous head")
    assert(polled.getNextInAccessQueue() === Entry.getNullEntry(),
      "The polled entry should be nullify access order")
    assert(polled.getPreviousInAccessQueue() === Entry.getNullEntry(),
      "The polled entry should be nullify access order")

    queue.clear()
    assert(queue.isEmpty, "Queue should be empty after clear")
    assert(queue.size() === 0, "The size of queue should be zero after clear")
    assert(!queue.iterator().hasNext, "The iterator returned by queue should be empty after clear")
  }

  def reHash(key: Int): Int = {
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
}
