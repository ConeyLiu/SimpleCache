package com.lxy

import java.util.concurrent.atomic.AtomicLong

case class CacheStatus(
    hitCount: Long,
    missCount: Long,
    loadCount: Long,
    totalLoadTime: Long,
    evictionCount: Long)

case class CacheStatusCounter(
    hitCounter: AtomicLong = new AtomicLong(0L),
    missCounter: AtomicLong = new AtomicLong(0L),
    loadCounter: AtomicLong = new AtomicLong(0L),
    totalLoadTimeCounter: AtomicLong = new AtomicLong(0L),
    evictionCounter: AtomicLong = new AtomicLong(0L)) {

  def snapshot(): CacheStatus = {
    CacheStatus(
      hitCounter.get(),
      missCounter.get(),
      loadCounter.get(),
      totalLoadTimeCounter.get(),
      evictionCounter.get()
    )
  }
}
