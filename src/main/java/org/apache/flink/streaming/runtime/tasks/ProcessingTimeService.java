/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Defines the current processing time and handles all related actions,
 * such as register timers for tasks to be executed in the future.
 *
 * <p>The access to the time via {@link #getCurrentProcessingTime()} is always available, regardless of
 * whether the timer service has been shut down.
 *
 * <p>The registration of timers follows a life cycle of three phases:
 * <ol>
 *     <li>In the initial state, it accepts timer registrations and triggers when the time is reached.</li>
 *     <li>After calling {@link #quiesce()}, further calls to
 *         {@link #registerTimer(long, ProcessingTimeCallback)} will not register any further timers, and will
 *         return a "dummy" future as a result. This is used for clean shutdown, where currently firing
 *         timers are waited for and no future timers can be scheduled, without causing hard exceptions.</li>
 *     <li>After a call to {@link #shutdownService()}, all calls to {@link #registerTimer(long, ProcessingTimeCallback)}
 *         will result in a hard exception.</li>
 * </ol>
 */
/**
 * 定义当前的进程时间以及处理所有相关的操作，比如注册定时器在未来执行
 * getCurrentProcessingTime 方法随时都可以调用，除非当服务挂掉了
 * 定时器的注册由三个阶段组成：
 * 在初始状态下，接受定时器注册，当时间到达的时候，触发定时器
 * 当调用 quiesce 方法，不接受新的定时器的注册，会返回 dummy
 * 当调用 shutdownService 方法，服务挂掉
 */
public abstract class ProcessingTimeService {

	/**
	 * Returns the current processing time.
	 */
	/**
	 * 返回当前的进程时间
	 */
	public abstract long getCurrentProcessingTime();

	/**
	 * Registers a task to be executed when (processing) time is {@code timestamp}.
	 *
	 * @param timestamp   Time when the task is to be executed (in processing time)
	 * @param target      The task to be executed
	 *
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	/**
	 * 注册一个定时器，当进程时间到达 ts，进行调度
	 */
	public abstract ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target);

	/**
	 * Registers a task to be executed repeatedly at a fixed rate.
	 *
	 * @param callback to be executed after the initial delay and then after each period
	 * @param initialDelay initial delay to start executing callback
	 * @param period after the initial delay after which the callback is executed
	 * @return Scheduled future representing the task to be executed repeatedly
	 */
	/**
	 * 注册一个定时器，按固定的速率循环被调度
	 * @param callback 等待调度的 task
	 * @param initialDelay 第一次调度需要等待的时间
	 * @param period 第一次调度之后的每次调度需要等待的时间
	 */
	public abstract ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period);

	/**
	 * Returns <tt>true</tt> if the service has been shut down, <tt>false</tt> otherwise.
	 */
	/**
	 * 返回当前的服务是否被关闭
	 */
	public abstract boolean isTerminated();

	/**
	 * This method puts the service into a state where it does not register new timers, but
	 * returns for each call to {@link #registerTimer(long, ProcessingTimeCallback)} only a "mock" future.
	 * Furthermore, the method clears all not yet started timers.
	 *
	 * <p>This method can be used to cleanly shut down the timer service. The using components
	 * will not notice that the service is shut down (as for example via exceptions when registering
	 * a new timer), but the service will simply not fire any timer any more.
	 */
	/**
	 * 这个方法让服务进入一个不接受定时器注册的状态，同时，这个方法清除
	 * 所有还没开始的定时器，这个方法会返回一个 mock future
	 * 使用方并不知道服务关闭了
	 */
	public abstract void quiesce() throws InterruptedException;

	/**
	 * This method can be used after calling {@link #quiesce()}, and awaits the completion
	 * of currently executing timers.
	 */
	/**
	 * 这个方法能够在 quiesce 方法被调用之后调用，等待所有现在在运行的定时器的完成
	 */
	public abstract void awaitPendingAfterQuiesce() throws InterruptedException;

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception.
	 */
	/**
	 * 直接关闭所有的定时器，会返回异常
	 */
	public abstract void shutdownService();

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception. This call cannot be interrupted and will block until the shutdown is completed
	 * or the timeout is exceeded.
	 *
	 * @param timeoutMs timeout for blocking on the service shutdown in milliseconds.
	 * @return returns true iff the shutdown was completed.
	 */
	/**
	 * 不能被打断的 shutdown 操作
	 * 方法会阻塞直到 shutdown 完成或者 ts 到了
	 */
	public abstract boolean shutdownServiceUninterruptible(long timeoutMs);

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does wait
	 * for all timers to complete or until the time limit is exceeded. Any call to
	 * {@link #registerTimer(long, ProcessingTimeCallback)} will result in a hard exception after calling this method.
	 * @param time time to wait for termination.
	 * @param timeUnit time unit of parameter time.
	 * @return {@code true} if this timer service and all pending timers are terminated and
	 *         {@code false} if the timeout elapsed before this happened.
	 */
	/**
	 * 等待现在执行的定时器完成之后执行 shutdown 操作
	 */
	public abstract boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException;
}
