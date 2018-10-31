package com.lxy

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ArrayBlockingQueue, Callable, Executors, Future => JFuture}
import java.util.function.IntUnaryOperator

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class LRUCacheSuite extends FunSuite with BeforeAndAfterAll {

  test("normal test with single thread") {
    val map = Map(1 -> 1, 2 -> 2, 3-> 3)
    val maxMemory = new AtomicInteger(6)
    val cache = Cache.lru[Int, Int](1, 8, 6,
      getPlainWeigher(),
      getPlainLoader(map, 4),
      getCacheHandler(maxMemory),
      getRemovalListener(maxMemory) :: Nil,
      new LockManagement[Int]())

    assert(!cache.contains(1))
    val value1 = cache.get(1)
    val value2 = cache.get(2)
    val value3 = cache.get(3)
    assert(value1 === Some(1))
    assert(value2 === Some(2))
    assert(value3 === Some(3))
    assert(cache.contains(1))
    assert(cache.contains(2))
    assert(cache.contains(3))

    val value4 = cache.get(5)
    assert(value4 === Some(4))
    assert(!cache.contains(1))
    assert(!cache.contains(2))
    assert(!cache.contains(3))
    val value5 = cache.remove(1)
    assert(value5 === None)
    cache.clear()
    assert(!cache.contains(5))
    assert(cache.size() === 0)
  }

  test("expand test") {
    val map = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5)
    val maxMemory = new AtomicInteger(6)
    val cache = Cache.lru[Int, Int](1, 1, 6,
      getPlainWeigher(),
      getPlainLoader(map, 6),
      getCacheHandler(maxMemory),
      getRemovalListener(maxMemory) :: Nil,
      new LockManagement[Int]())

    assert(cache.size() === 0)
    (1 until 6).foreach(i => assert(!cache.contains(i)))

    (1 until 6).foreach(i => assert(cache.get(i) === map.get(i)))

    assert(cache.get(6).get === 6)

    (1 until 6).foreach(i => assert(!cache.contains(i)))

    cache.clear()
    assert(cache.size() === 0)
  }

  test("test multi-listeners with single thread") {
    val buffer = new ArrayBlockingQueue[(Int, Int)](4)
    val listener = new RemoveListener[Int, Int] {
      override def onRemove(key: Int, value: Int): Unit = {
        buffer.offer((key, value))
      }
    }
    val map = Map(1 -> 1, 2 -> 2, 3-> 3)
    val maxMemory = new AtomicInteger(6)
    val cache = Cache.lru[Int, Int](1, 8, 6,
      getPlainWeigher(),
      getPlainLoader(map, 4),
      getCacheHandler(maxMemory),
      getRemovalListener(maxMemory) :: listener :: Nil,
      new LockManagement[Int]())

    cache.get(1)
    cache.get(2)
    cache.get(3)
    cache.get(4)

    assert(cache.contains(4))
    assert(buffer.size === 3)

    cache.clear()
    assert(cache.size() === 0)
    Thread.sleep(1000)
    assert(buffer.size === 4)
    // We used multi-thread to process the remove listeners, so the order need be sorted.
    val result = buffer.asScala.toArray.sortBy(_._1)
    val expected = Array((1, 1), (2, 2), (3, 3), (4, 4))
    assert(result === expected)
    buffer.clear()
  }

  /**
   * This test is a little flaky, it need re-design.
   */
  test("normal test with multi-threads") {
    val map = Map(1 -> 1, 2 -> 2, 3-> 3, 4 -> 4, 5 -> 5)
    val maxMemory = new AtomicInteger(12)
    val cache = Cache.lru[Int, Int](2, 8, 12,
      getPlainWeigher(),
      getPlainLoader(map, 6),
      getCacheHandler(maxMemory),
      getRemovalListener(maxMemory) :: Nil,
      new LockManagement[Int]())
    implicit val context: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))
    val tasks = (1 until 8).map { i =>
      Future {
        cache.get(i).get
      }(context)
    }

    val results = Await.result(Future.sequence(tasks), Duration.Inf).toArray
    assert(results === Array(1, 2, 3, 4, 5, 6, 6))
    cache.clear()
  }


  def getPlainWeigher(): Weigher[Int, Int] = {
    new Weigher[Int, Int] {
      override def weight(key: Int, value: Int): Int = value
    }
  }

  def getCacheHandler(totalMemory: AtomicInteger): CacheHandler[Int, Int] = {
    new CacheHandler[Int, Int] {
      override def allocate(key: Int, value: Int): Long = {
        val address = new AtomicLong(0L)
        totalMemory.updateAndGet(new IntUnaryOperator{
          override def applyAsInt(remaining: Int): Int = {
            if (remaining >= value) {
              address.set(1L)
              remaining - value
            } else {
              address.set(0L)
              remaining
            }
          }
        })

        address.get()
      }

      override def cache(key: Int, value: Int, address: Long): Boolean = {
        if (address != 0) {
          true
        } else {
          false
        }
      }
    }
  }

  def getRemovalListener(totalMemory: AtomicInteger): RemoveListener[Int, Int] = {
    new RemoveListener[Int, Int] {
      override def onRemove(key: Int, value: Int): Unit = {
        totalMemory.addAndGet(value)
      }
    }
  }

  def getPlainLoader(map: Map[Int, Int], defaultValue: Int): Loader[Int, Int] = {
    new Loader[Int, Int] {
      override def load(key: Int): Int = map.get(key).getOrElse(defaultValue)
    }
  }
}
