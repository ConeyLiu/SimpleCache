package com.lxy

import java.util.concurrent.{Executor, Executors, ThreadFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

abstract class ValueReference[K: ClassTag, V: ClassTag] {
  def getValue(): Option[V]
  def waitForValue(): Option[V]
  def getWeight(): Int
  def isLoading(): Boolean
  def isActive(): Boolean
}

class NormalValueReference[K: ClassTag, V: ClassTag](
    value: V,
    weight: Int)
  extends ValueReference[K, V] {
  override def getValue(): Option[V] = Some(value)

  override def waitForValue(): Option[V] = Some(value)

  override def getWeight(): Int = weight

  override def isLoading(): Boolean = false

  override def isActive(): Boolean = true
}

/**
  * This is used in async loading property. Currently it is not supported.
  */
class LoadingValueReference[K: ClassTag, V: ClassTag] (
    key: K,
    weight: Int,
    loader: Loader[K, V]) extends ValueReference[K, V] {

  private val pool: Executor = Executors.newSingleThreadExecutor((r: Runnable) => {
    Thread.currentThread()
  })

  val context: ExecutionContext = ExecutionContext.fromExecutor(pool)
  private val _value = Future(loader.load(key))(context)
  override def getValue(): Option[V] = None

  override def waitForValue(): Option[V] = Option(Await.result(_value, Duration.Inf))

  override def getWeight(): Int = weight

  override def isLoading(): Boolean = true

  override def isActive(): Boolean = false
}
