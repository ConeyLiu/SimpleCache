package com.lxy

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.{Function => JFunction}

class LockManagement[K] {
	private val keyToLock = new ConcurrentHashMap[K, ReentrantReadWriteLock]()

	def getLock(key: K): ReentrantReadWriteLock = {
		keyToLock.computeIfAbsent(key, new JFunction[K, ReentrantReadWriteLock] {
			override def apply(t: K): ReentrantReadWriteLock = new ReentrantReadWriteLock()
		})
	}

	def removeLock(key: K): Unit = {
		keyToLock.remove(key)
	}

	def clear(): Unit = {
		keyToLock.clear()
	}
}
