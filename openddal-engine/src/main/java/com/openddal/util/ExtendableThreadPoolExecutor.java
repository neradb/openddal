/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.util;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Same as a java.util.concurrent.ThreadPoolExecutor but implements a much more efficient
 * {@link #getSubmittedCount()} method, to be used to properly handle the work queue.
 * If a RejectedExecutionHandler is not specified a default one will be configured
 * and that one will always throw a RejectedExecutionException
 *
 */
public class ExtendableThreadPoolExecutor extends ThreadPoolExecutor {

    /**
     * The number of tasks submitted but not yet finished. This includes tasks
     * in the queue and tasks that have been handed to a worker thread but the
     * latter did not start executing the task yet.
     * This number is always greater or equal to {@link #getActiveCount()}.
     */
    private final AtomicInteger submittedCount = new AtomicInteger(0);

    public ExtendableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, TaskQueue workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new RejectHandler());
        workQueue.setParent(this);
    }


    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        submittedCount.decrementAndGet();
    }


    public int getSubmittedCount() {
        return submittedCount.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        execute(command,0,TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the given command at some time in the future.  The command
     * may execute in a new thread, in a pooled thread, or in the calling
     * thread, at the discretion of the <tt>Executor</tt> implementation.
     * If no threads are available, it will be added to the work queue.
     * If the work queue is full, the system will wait for the specified
     * time and it throw a RejectedExecutionException if the queue is still
     * full after that.
     *
     * @param command the runnable task
     * @param timeout A timeout for the completion of the task
     * @param unit The timeout time unit
     * @throws RejectedExecutionException if this task cannot be
     * accepted for execution - the queue is full
     * @throws NullPointerException if command or unit is null
     */
    public void execute(Runnable command, long timeout, TimeUnit unit) {
        submittedCount.incrementAndGet();
        try {
            super.execute(command);
        } catch (RejectedExecutionException rx) {
            if (super.getQueue() instanceof TaskQueue) {
                final TaskQueue queue = (TaskQueue)super.getQueue();
                try {
                    if (!queue.force(command, timeout, unit)) {
                        submittedCount.decrementAndGet();
                        throw new RejectedExecutionException("Queue capacity is full.");
                    }
                } catch (InterruptedException x) {
                    submittedCount.decrementAndGet();
                    throw new RejectedExecutionException(x);
                }
            } else {
                submittedCount.decrementAndGet();
                throw rx;
            }

        }
    }


    private static class RejectHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r,
                java.util.concurrent.ThreadPoolExecutor executor) {
            throw new RejectedExecutionException();
        }

    }

    
    /**
     * As task queue specifically designed to run with a thread pool executor. The
     * task queue is optimised to properly utilize threads within a thread pool
     * executor. If you use a normal queue, the executor will spawn threads when
     * there are idle threads and you wont be able to force items onto the queue
     * itself.
     */
    public static class TaskQueue extends LinkedBlockingQueue<Runnable> {

        private static final long serialVersionUID = 1L;

        private volatile ExtendableThreadPoolExecutor parent = null;

        // No need to be volatile. This is written and read in a single thread
        // (when stopping a context and firing the  listeners)
        private Integer forcedRemainingCapacity = null;

        public TaskQueue() {
            super();
        }

        public TaskQueue(int capacity) {
            super(capacity);
        }

        public TaskQueue(Collection<? extends Runnable> c) {
            super(c);
        }

        public void setParent(ExtendableThreadPoolExecutor tp) {
            parent = tp;
        }

        public boolean force(Runnable o) {
            if ( parent==null || parent.isShutdown() ){
                throw new RejectedExecutionException("Executor not running, can't force a command into the queue");
            }
            return super.offer(o); //forces the item onto the queue, to be used if the task is rejected
        }

        public boolean force(Runnable o, long timeout, TimeUnit unit) throws InterruptedException {
            if ( parent==null || parent.isShutdown() ){
                throw new RejectedExecutionException("Executor not running, can't force a command into the queue");
            }
            return super.offer(o,timeout,unit); //forces the item onto the queue, to be used if the task is rejected
        }

        @Override
        public synchronized boolean offer(Runnable o) {
          //we can't do any checks
            if (parent==null){
                return super.offer(o);
            }
            //we are maxed out on threads, simply queue the object
            if (parent.getPoolSize() == parent.getMaximumPoolSize()){
                return super.offer(o);
            }
            //we have idle threads, just add it to the queue
            if (parent.getSubmittedCount()<(parent.getPoolSize())){
                return super.offer(o);
            }
            //if we have less threads than maximum force creation of a new thread
            if (parent.getPoolSize()<parent.getMaximumPoolSize()){
                return false;
            }
            //if we reached here, we need to add it to the queue
            return super.offer(o);
        }

        @Override
        public int remainingCapacity() {
            if (forcedRemainingCapacity != null) {
                // ThreadPoolExecutor.setCorePoolSize checks that
                // remainingCapacity==0 to allow to interrupt idle threads
                // I don't see why, but this hack allows to conform to this
                // "requirement"
                return forcedRemainingCapacity.intValue();
            }
            return super.remainingCapacity();
        }

        public void setForcedRemainingCapacity(Integer forcedRemainingCapacity) {
            this.forcedRemainingCapacity = forcedRemainingCapacity;
        }

    }


}
