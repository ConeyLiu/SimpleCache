package com.lxy

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

class LockManagement[K] {
	private val keyToLock = new ConcurrentHashMap[K, ReadWriteLock]()

	def getLock(key: K): ReadWriteLock = {
		keyToLock.computeIfAbsent(key, _ => new ReentrantReadWriteLock())
	}

	def removeLock(key: K): Unit = {
		keyToLock.remove(key)
	}

	def clear(): Unit = {
		keyToLock.clear()
	}
}
