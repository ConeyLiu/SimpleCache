package com.lxy

import java.util.concurrent.{ArrayBlockingQueue, Callable, Executors, Future => JFuture}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class LRUCacheSuite extends FunSuite with BeforeAndAfterAll {

  test("normal test with single thread") {
    val map = Map(1 -> 2, 2 -> 3, 3-> 4)
    val cache = Cache.lru[Int, Int](1, 8, 3,
      getPlainWeigher(),
      getPlainLoader(map, 10),
      getPlainSatisfy(),
      Nil)

    assert(!cache.contains(1))
    val value1 = cache.get(1)
    val value2 = cache.get(2)
    val value3 = cache.get(5)
    assert(value1 == Some(2))
    assert(value2 == Some(3))
    assert(value3 == Some(10))
    assert(cache.contains(1))
    assert(cache.contains(2))
    assert(cache.contains(5))
    cache.remove(1)
    assert(!cache.contains(1))
    assert(cache.contains(2))
    assert(cache.contains(5))
    cache.clear()
    assert(!cache.contains(2))
    assert(!cache.contains(5))
  }

  test("LRU algorithm") {
    val map = Map(1 -> 2, 2 -> 3, 3-> 4)
    val cache = Cache.lru[Int, Int](1, 8, 3,
      getPlainWeigher(),
      getPlainLoader(map, 10),
      getPlainSatisfy(),
      Nil)

    cache.get(1)
    assert(cache.contains(1))
    cache.get(2)
    assert(cache.contains(2))
    cache.get(3)
    assert(cache.contains(3))
    cache.get(4)
    assert(cache.contains(4))
    assert(!cache.contains(1))

    cache.clear()
  }

  test("test listener with single thread") {
    val buffer = new ArrayBlockingQueue[(Int, Int)](4)
    val listener = new RemoveListener[Int, Int] {
      override def onRemove(key: Int, value: Int): Unit = {
        buffer.offer((key, value))
      }
    }
    val map = Map(1 -> 2, 2 -> 3, 3-> 4)
    val cache = Cache.lru[Int, Int](1, 8, 3,
      getPlainWeigher(),
      getPlainLoader(map, 10),
      getPlainSatisfy(),
      Seq(listener))

    // Wait the remove thread start work
    Thread.sleep(1000)

    cache.get(1)
    cache.get(2)
    cache.get(3)
    cache.get(4)
    // wait the remove thread to remove the entry
    Thread.sleep(1000)
    assert(!cache.contains(1))
    assert(buffer.size === 1)
    assert(buffer.peek() === (1, 2))

    cache.clear()
    Thread.sleep(3000)
    assert(buffer.size === 4)
    // We used multi-thread to reslove the remove listeners, so the order need be sorted.
    val result = buffer.asScala.toArray.sortBy(_._1)
    val expected = Array((1, 2), (2, 3), (3, 4), (4, 10))
    assert(result === expected)
  }

  test("normal test with multi-threads") {
    val map = Map(1 -> 2, 2 -> 3, 3-> 4)
    val cache = Cache.lru[Int, Int](2, 8, 4,
      getPlainWeigher(),
      getPlainLoader(map, 10),
      getPlainSatisfy(),
      Nil)

    implicit val context: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))
    val tasks = (1 until 4).map { i =>
      Future {
        cache.get(i).get
      }(context)
    }

    val results = Await.result(Future.sequence(tasks), Duration.Inf).toArray
    assert(results === Array(2, 3, 4))
    cache.clear()
  }

  def getPlainWeigher[K, V](defaultValue: Int = 1): Weigher[K, V] = {
    new Weigher[K, V] {
      override def weight(key: K, value: V): Int = defaultValue
    }
  }

  def getPlainSatisfy[K, V](defaultValue: Boolean = true): Satisfy[K, V] = {
    new Satisfy[K, V] {
      override def check(key: K, value: V, weight: Int): Boolean = defaultValue
    }
  }

  def getPlainLoader[K, V](map: Map[K, V], defaultValue: V): Loader[K, V] = {
    new Loader[K, V] {
      override def load(key: K): V = map.get(key).getOrElse(defaultValue)
    }
  }

}
