/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.LinkedList;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Manages the read/write consistency within memstore. This provides
 * an interface for readers to determine what entries to ignore, and
 * a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
public class MultiVersionConsistencyControl {
  private volatile long memstoreRead = 0;
  private volatile long memstoreWrite = 0;

  private final Object readWaiters = new Object();

  // This is the pending queue of writes.
  private final LinkedList<WriteEntry> writeQueue =
      new LinkedList<WriteEntry>();

  private static final ThreadLocal<Long> perThreadReadPoint =
      new ThreadLocal<Long>() {
       @Override
      protected
       Long initialValue() {
         return Long.MAX_VALUE;
       }
  };

  /**
   * Default constructor. Initializes the memstoreRead/Write points to 0.
   */
  public MultiVersionConsistencyControl() {
    this.memstoreRead = this.memstoreWrite = 0;
  }

  /**
   * Initializes the memstoreRead/Write points appropriately.
   * @param startPoint
   */
  public void initialize(long startPoint) {
    synchronized (writeQueue) {
      if (this.memstoreWrite != this.memstoreRead) {
        throw new RuntimeException("Already used this mvcc. Too late to initialize");
      }

      this.memstoreRead = this.memstoreWrite = startPoint;
    }
  }

  /**
   * Get this thread's read point. Used primarily by the memstore scanner to
   * know which values to skip (ie: have not been completed/committed to
   * memstore).
   */
  public static long getThreadReadPoint() {
      return perThreadReadPoint.get();
  }

  /**
   * Set the thread read point to the given value. The thread MVCC
   * is used by the Memstore scanner so it knows which values to skip.
   * Give it a value of 0 if you want everything.
   */
  public static void setThreadReadPoint(long readPoint) {
    perThreadReadPoint.set(readPoint);
  }

  /**
   * Set the thread MVCC read point to whatever the current read point is in
   * this particular instance of MVCC.  Returns the new thread read point value.
   */
  public static long resetThreadReadPoint(MultiVersionConsistencyControl mvcc) {
    perThreadReadPoint.set(mvcc.memstoreReadPoint());
    return getThreadReadPoint();
  }

  /**
   * Set the thread MVCC read point to 0 (include everything).
   */
  public static void resetThreadReadPoint() {
    perThreadReadPoint.set(0L);
  }

  public WriteEntry beginMemstoreInsert() {
	 //加锁，保证有序一致性
    synchronized (writeQueue) {
      long nextWriteNumber = ++memstoreWrite;
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      return e;
    }
  }

  public void completeMemstoreInsert(WriteEntry e) {
    advanceMemstore(e);
    waitForRead(e);
  }

  boolean advanceMemstore(WriteEntry e) {
	  //先入队的事务不一定先完成，所以进入这个方法的有可能是后续的后入队的事务
    synchronized (writeQueue) {
      e.markCompleted();

      long nextReadValue = -1;
      boolean ranOnce=false;
      while (!writeQueue.isEmpty()) {
        ranOnce=true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue+1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("invariant in completeMemstoreInsert violated, prev: "
                + nextReadValue + " next: " + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
         //一直循环到第一个没有完成的任务
          break;
        }
      }
      //如果队列为空，不可能进来
      if (!ranOnce) {
        throw new RuntimeException("never was a first");
      }
      //有新事务完成了，可以重置读点，并且通知等待读写点更新到指定值的另外的写线程
      if (nextReadValue > 0) {
        synchronized (readWaiters) {
          memstoreRead = nextReadValue;
          readWaiters.notifyAll();
        }
      }
      //有可能进来的是后续的序号的任务，那么读点比当前的序号小
      if (memstoreRead >= e.getWriteNumber()) {
        return true;
      }
      return false;
    }
  }

  /**
   * Wait for the global readPoint to advance upto
   * the specified transaction number.
   *当前WriteEntry可能是后入队的事务结果先执行完了，那么读写点比当前的事务小，那么一直等待，
   *如果不等待，那么MVCC读不到这条数据
   */
  public void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    synchronized (readWaiters) {
      while (memstoreRead < e.getWriteNumber()) {
        try {
          readWaiters.wait(0);
        } catch (InterruptedException ie) {
          // We were interrupted... finish the loop -- i.e. cleanup --and then
          // on our way out, reset the interrupt flag.
          interrupted = true;
        }
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  public long memstoreReadPoint() {
    return memstoreRead;
  }


  public static class WriteEntry {
    private long writeNumber;
    private boolean completed = false;
    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }
    void markCompleted() {
      this.completed = true;
    }
    boolean isCompleted() {
      return this.completed;
    }
    long getWriteNumber() {
      return this.writeNumber;
    }
  }

  public static final long FIXED_SIZE = ClassSize.align(
      ClassSize.OBJECT +
      2 * Bytes.SIZEOF_LONG +
      2 * ClassSize.REFERENCE);

}
