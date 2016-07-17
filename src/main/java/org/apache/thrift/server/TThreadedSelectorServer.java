/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.server;

import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 一个半同步/半异步服务器,通过一个独立的线程池来处理非阻塞I/O.使用单个线程来处理Accept请求,然后
 * 一个配置数量的非阻塞selector线程来管理客户端过来的read和write操作.然后,一个同步worker线程池来处理请求的流程.
 *
 * TNonblockingServer 是单个线程selector模式处理连接非阻塞I/O的.
 *
 * 在多核环境下,由于基于单个selector线程处理I/O时,CPU会是一个瓶颈,所以TThreadedSelectorServer性能比TNonblockingServer/THsHaServer好.
 * 此外,由于accept处理 和 read/write调用解耦,server可以有更好的能力处理新的连接压力(比如服务繁忙的时候停止accept).
 *
 * 和 TNonblockingServer 一样,底层必须使用 TFramedTransport 传输协议
 */
public class TThreadedSelectorServer extends AbstractNonblockingServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TThreadedSelectorServer.class.getName());

  // 配置信息
  // 增加了worker线程池,并且selector线程个数增加
  public static class Args extends AbstractNonblockingServerArgs<Args> {

    /** The number of threads for selecting on already-accepted connections */
    public int selectorThreads = 2;
    /**
     * The size of the executor service (if none is specified) that will handle
     * invocations. This may be set to 0, in which case invocations will be
     * handled directly on the selector threads (as is in TNonblockingServer)
     */
    private int workerThreads = 5;
    /** Time to wait for server to stop gracefully */
    private int stopTimeoutVal = 60;
    private TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
    /** 分发请求的线程池 The ExecutorService for handling dispatched requests */
    private ExecutorService executorService = null;
    /**
     * selector 线程池的队列大小
     * The size of the blocking queue per selector thread for passing accepted
     * connections to the selector thread
     */
    private int acceptQueueSizePerThread = 4;

    /**
     * 处理新accepted连接的策略 Determines the strategy for handling new accepted connections.
     */
    public static enum AcceptPolicy {
      /**
       * Require accepted connection registration to be handled by the executor.
       * If the worker pool is saturated, further accepts will be closed
       * immediately. Slightly increases latency due to an extra scheduling.
       */
      FAIR_ACCEPT, //如果worker池饱和了,则更多地accept将会快速被关闭.
      /**
       * Handle the accepts as fast as possible, disregarding the status of the
       * executor service.
       */
      FAST_ACCEPT // 快速处理,不管线程池的任务异常
    }

    private AcceptPolicy acceptPolicy = AcceptPolicy.FAST_ACCEPT;

    public Args(TNonblockingServerTransport transport) {
      super(transport);
    }

    public Args selectorThreads(int i) {
      selectorThreads = i;
      return this;
    }

    public int getSelectorThreads() {
      return selectorThreads;
    }

    public Args workerThreads(int i) {
      workerThreads = i;
      return this;
    }

    public int getWorkerThreads() {
      return workerThreads;
    }

    public int getStopTimeoutVal() {
      return stopTimeoutVal;
    }

    public Args stopTimeoutVal(int stopTimeoutVal) {
      this.stopTimeoutVal = stopTimeoutVal;
      return this;
    }

    public TimeUnit getStopTimeoutUnit() {
      return stopTimeoutUnit;
    }

    public Args stopTimeoutUnit(TimeUnit stopTimeoutUnit) {
      this.stopTimeoutUnit = stopTimeoutUnit;
      return this;
    }

    public ExecutorService getExecutorService() {
      return executorService;
    }

    public Args executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public int getAcceptQueueSizePerThread() {
      return acceptQueueSizePerThread;
    }

    public Args acceptQueueSizePerThread(int acceptQueueSizePerThread) {
      this.acceptQueueSizePerThread = acceptQueueSizePerThread;
      return this;
    }

    public AcceptPolicy getAcceptPolicy() {
      return acceptPolicy;
    }

    public Args acceptPolicy(AcceptPolicy acceptPolicy) {
      this.acceptPolicy = acceptPolicy;
      return this;
    }

    public void validate() {
      if (selectorThreads <= 0) {
        throw new IllegalArgumentException("selectorThreads must be positive.");
      }
      if (workerThreads < 0) {
        throw new IllegalArgumentException("workerThreads must be non-negative.");
      }
      if (acceptQueueSizePerThread <= 0) {
        throw new IllegalArgumentException("acceptQueueSizePerThread must be positive.");
      }
    }
  } // END Args

  // The thread handling all accepts
  private AcceptThread acceptThread;// 单个accept线程,处理所有accept请求

  // Threads handling events on client transports
  private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();

  //封装所有的入队函数,以及为提供将invocation从selector线程传递到worker线程的线程池管理
  private final ExecutorService invoker;

  private final Args args;

  /**
   * Create the server with the specified Args configuration
   */
  public TThreadedSelectorServer(Args args) {
    super(args);
    args.validate();
    invoker = args.executorService == null ? createDefaultExecutor(args) : args.executorService;
    this.args = args;
  }

  /**
   * 启动accept线程 和 selector线程处理clients请求.先启动selector线程,然后accept线程启动开始接受外部连接
   *
   * @return true if everything went ok, false if we couldn't start for some
   *         reason.
   */
  @Override
  protected boolean startThreads() {
    try {
      for (int i = 0; i < args.selectorThreads; ++i) {
        selectorThreads.add(new SelectorThread(args.acceptQueueSizePerThread));
      }
      acceptThread = new AcceptThread((TNonblockingServerTransport) serverTransport_,
        createSelectorThreadLoadBalancer(selectorThreads));//RR轮询selectorThreads
      for (SelectorThread thread : selectorThreads) {
        thread.start();
      }
      acceptThread.start();//启动accept和selector线程
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to start threads!", e);
      return false;
    }
  }

  /**
   * Joins the accept and selector threads and shuts down the executor service.
   */
  @Override
  protected void waitForShutdown() {
    try {
      joinThreads();
    } catch (InterruptedException e) {
      // Non-graceful shutdown occurred
      LOGGER.error("Interrupted while joining threads!", e);
    }
    gracefullyShutdownInvokerPool();
  }

  // 等待所有启动的线程执行完成,再退出
  protected void joinThreads() throws InterruptedException {
    // wait until the io threads exit
    acceptThread.join();
    for (SelectorThread thread : selectorThreads) {
      thread.join();
    }
  }

  /**
   * Stop serving and shut everything down.
   */
  @Override
  public void stop() {
    stopped_ = true;

    // Stop queuing connect attempts asap
    stopListening(); // 停止监听socket,也就是停止新的connect入队

    if (acceptThread != null) {
      acceptThread.wakeupSelector();
    }
    if (selectorThreads != null) {
      for (SelectorThread thread : selectorThreads) {
        if (thread != null)
          thread.wakeupSelector();
      }
    }
  }

  protected void gracefullyShutdownInvokerPool() {
    // try to gracefully shut down the executor service
    invoker.shutdown();//关闭连接池

    // Loop until awaitTermination finally does return without a interrupted
    // exception. If we don't do this, then we'll shut down prematurely. We want
    // to let the executorService clear it's task queue, closing client sockets
    // appropriately.
    long timeoutMS = args.stopTimeoutUnit.toMillis(args.stopTimeoutVal);
    long now = System.currentTimeMillis();
    while (timeoutMS >= 0) {
      try {
        invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
        break;
      } catch (InterruptedException ix) {
        long newnow = System.currentTimeMillis();
        timeoutMS -= (newnow - now);
        now = newnow;
      }
    }
  }

  /**
   * We override the standard invoke method here to queue the invocation for
   * invoker service instead of immediately invoking. If there is no thread
   * pool, handle the invocation inline on this thread
   */
  @Override
  protected boolean requestInvoke(FrameBuffer frameBuffer) {
    Runnable invocation = getRunnable(frameBuffer);
    if (invoker != null) {
      try {
        invoker.execute(invocation);//线程池执行具体的调研
        return true;
      } catch (RejectedExecutionException rx) {
        LOGGER.warn("ExecutorService rejected execution!", rx);
        return false;
      }
    } else {
      // Invoke on the caller's thread
      invocation.run();//不使用线程池,则对象线程自己执行invoke
      return true;
    }
  }

  // 直接封装buffer的invoke为一个runnable对象
  protected Runnable getRunnable(FrameBuffer frameBuffer) {
    return new Invocation(frameBuffer);
  }

  /**
   * Helper to create the invoker if one is not specified
   */
  protected static ExecutorService createDefaultExecutor(Args options) {
    return (options.workerThreads > 0) ? Executors.newFixedThreadPool(options.workerThreads) : null;
  }

  private static BlockingQueue<TNonblockingTransport> createDefaultAcceptQueue(int queueSize) {
    if (queueSize == 0) {
      // Unbounded queue
      return new LinkedBlockingQueue<TNonblockingTransport>();
    }
    return new ArrayBlockingQueue<TNonblockingTransport>(queueSize);
  }

  /**
   * 该线程是接收服务器transport连接,然后交给IO selector线程池来处理
   * The thread that selects on the server transport (listen socket) and accepts
   * new connections to hand off to the IO selector threads
   */
  protected class AcceptThread extends Thread {

    // The listen socket to accept on
    private final TNonblockingServerTransport serverTransport;
    private final Selector acceptSelector;

    private final SelectorThreadLoadBalancer threadChooser;//从selector池子里 RR 选出一个 selector来

    /**
     * Set up the AcceptThead
     * 
     * @throws IOException
     */
    public AcceptThread(TNonblockingServerTransport serverTransport,
        SelectorThreadLoadBalancer threadChooser) throws IOException {
      this.serverTransport = serverTransport;
      this.threadChooser = threadChooser;
      this.acceptSelector = SelectorProvider.provider().openSelector();
      this.serverTransport.registerSelector(acceptSelector);
    }

    /**
     * The work loop. Selects on the server transport and accepts. If there was
     * a server transport that had blocking accepts, and returned on blocking
     * client transports, that should be used instead
     */
    public void run() {
      try {
        if (eventHandler_ != null) {
          eventHandler_.preServe(); // 自定义预处理方法
        }

        while (!stopped_) {
          select();
        }
      } catch (Throwable t) {
        LOGGER.error("run() on AcceptThread exiting due to uncaught error", t);
      } finally {
        try {
          acceptSelector.close();
        } catch (IOException e) {
          LOGGER.error("Got an IOException while closing accept selector!", e);
        }
        // This will wake up the selector threads
        TThreadedSelectorServer.this.stop();
      }
    }

    /**
     * If the selector is blocked, wake it up.
     */
    public void wakeupSelector() {
      acceptSelector.wakeup();
    }

    /**
     * select 然后处理IO事件
     *
     * Select and process IO events appropriately: If there are connections to
     * be accepted, accept them.
     */
    private void select() {
      try {
        // wait for connect events.
        acceptSelector.select();

        // process the io events we received
        Iterator<SelectionKey> selectedKeys = acceptSelector.selectedKeys().iterator();
        while (!stopped_ && selectedKeys.hasNext()) {
          SelectionKey key = selectedKeys.next();
          selectedKeys.remove();

          // skip if not valid
          if (!key.isValid()) {
            continue;
          }

          // 只处理 accept相关的事件
          if (key.isAcceptable()) {
            handleAccept();
          } else {
            LOGGER.warn("Unexpected state in select! " + key.interestOps());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Got an IOException while selecting!", e);
      }
    }

    /**
     * 接收一个新的 connection.
     */
    private void handleAccept() {
      final TNonblockingTransport client = doAccept();//调用transport的accept方法
      if (client != null) {
        // Pass this connection to a selector thread
        final SelectorThread targetThread = threadChooser.nextThread();//选出一个selector线程

        if (args.acceptPolicy == Args.AcceptPolicy.FAST_ACCEPT || invoker == null) {
          doAddAccept(targetThread, client);// 加入到selector的queue中
        } else {
          // FAIR_ACCEPT
          try {
            invoker.submit(new Runnable() {
              public void run() {
                doAddAccept(targetThread, client);//交给线程池处理,如果queue已满,则关闭client连接
              }
            });
          } catch (RejectedExecutionException rx) {
            LOGGER.warn("ExecutorService rejected accept registration!", rx);
            // close immediately
            client.close();
          }
        }
      }
    }

    private TNonblockingTransport doAccept() {
      try {
        return (TNonblockingTransport) serverTransport.accept();
      } catch (TTransportException tte) {
        // something went wrong accepting.
        LOGGER.warn("Exception trying to accept!", tte);
        return null;
      }
    }

    // accept事件加入到accepted连接的队列中
    private void doAddAccept(SelectorThread thread, TNonblockingTransport client) {
      if (!thread.addAcceptedConnection(client)) {
        client.close();
      }
    }
  } // AcceptThread

  /**
   * The SelectorThread(s) will be doing all the selecting on accepted active
   * connections.
   */
  protected class SelectorThread extends AbstractSelectThread {

    // Accepted connections added by the accept thread.
    private final BlockingQueue<TNonblockingTransport> acceptedQueue;

    /**
     * Set up the SelectorThread with an unbounded queue for incoming accepts.
     * 
     * @throws IOException
     *           if a selector cannot be created
     */
    public SelectorThread() throws IOException {
      this(new LinkedBlockingQueue<TNonblockingTransport>());
    }

    /**
     * Set up the SelectorThread with an bounded queue for incoming accepts.
     * 
     * @throws IOException
     *           if a selector cannot be created
     */
    public SelectorThread(int maxPendingAccepts) throws IOException {
      this(createDefaultAcceptQueue(maxPendingAccepts));
    }

    /**
     * Set up the SelectorThread with a specified queue for connections.
     * 
     * @param acceptedQueue
     *          The BlockingQueue implementation for holding incoming accepted
     *          connections.
     * @throws IOException
     *           if a selector cannot be created.
     */
    public SelectorThread(BlockingQueue<TNonblockingTransport> acceptedQueue) throws IOException {
      this.acceptedQueue = acceptedQueue;
    }

    /**
     * Hands off an accepted connection to be handled by this thread. This
     * method will block if the queue for new connections is at capacity.
     * 
     * @param accepted
     *          The connection that has been accepted.
     * @return true if the connection has been successfully added.
     */
    public boolean addAcceptedConnection(TNonblockingTransport accepted) {
      try {
        acceptedQueue.put(accepted);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while adding accepted connection!", e);
        return false;
      }
      selector.wakeup();//唤醒吹
      return true;
    }

    /**
     * 工作的主流程逻辑.处理 读写IO/分发 ,以及管理selection选择
     * The work loop. Handles selecting (read/write IO), dispatching, and
     * managing the selection preferences of all existing connections.
     */
    public void run() {
      try {
        while (!stopped_) {
          select();
          processAcceptedConnections();
          processInterestChanges();
        }
        for (SelectionKey selectionKey : selector.keys()) {
          cleanupSelectionKey(selectionKey);
        }
      } catch (Throwable t) {
        LOGGER.error("run() on SelectorThread exiting due to uncaught error", t);
      } finally {
        try {
          selector.close();
        } catch (IOException e) {
          LOGGER.error("Got an IOException while closing selector!", e);
        }
        // This will wake up the accept thread and the other selector threads
        TThreadedSelectorServer.this.stop();
      }
    }

    /**
     * Select and process IO events appropriately: If there are existing
     * connections with data waiting to be read, read it, buffering until a
     * whole frame has been read. If there are any pending responses, buffer
     * them until their target client is available, and then send the data.
     */
    private void select() {
      try {
        // wait for io events.
        selector.select();

        // process the io events we received
        Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
        while (!stopped_ && selectedKeys.hasNext()) {
          SelectionKey key = selectedKeys.next();
          selectedKeys.remove();

          // skip if not valid
          if (!key.isValid()) {
            cleanupSelectionKey(key);
            continue;
          }

          // 这里只selector的读写事件,accept事件由单独线程处理
          if (key.isReadable()) {
            // deal with reads
            handleRead(key);//读处理,则会调用方法
          } else if (key.isWritable()) {
            // deal with writes
            handleWrite(key);
          } else {
            LOGGER.warn("Unexpected state in select! " + key.interestOps());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Got an IOException while selecting!", e);
      }
    }

    private void processAcceptedConnections() {
      // 注册 accepted connections
      while (!stopped_) {
        TNonblockingTransport accepted = acceptedQueue.poll();
        if (accepted == null) {
          break;
        }
        registerAccepted(accepted);// 注册读事件,绑定framebuffer 到 attach
      }
    }

    protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans,
        final SelectionKey selectionKey,
        final AbstractSelectThread selectThread) {
        return processorFactory_.isAsyncProcessor() ?
                  new AsyncFrameBuffer(trans, selectionKey, selectThread) :
                  new FrameBuffer(trans, selectionKey, selectThread);
    }

    private void registerAccepted(TNonblockingTransport accepted) {
      SelectionKey clientKey = null;
      try {
        clientKey = accepted.registerSelector(selector, SelectionKey.OP_READ);

        FrameBuffer frameBuffer = createFrameBuffer(accepted, clientKey, SelectorThread.this);

        clientKey.attach(frameBuffer);
      } catch (IOException e) {
        LOGGER.warn("Failed to register accepted connection to selector!", e);
        if (clientKey != null) {
          cleanupSelectionKey(clientKey);
        }
        accepted.close();
      }
    }
  } // SelectorThread

  /**
   * Creates a SelectorThreadLoadBalancer to be used by the accept thread for
   * assigning newly accepted connections across the threads.
   */
  protected SelectorThreadLoadBalancer createSelectorThreadLoadBalancer(Collection<? extends SelectorThread> threads) {
    return new SelectorThreadLoadBalancer(threads);
  }

  /**
   * A round robin load balancer for choosing selector threads for new
   * connections.
   */
  protected static class SelectorThreadLoadBalancer {
    private final Collection<? extends SelectorThread> threads;
    private Iterator<? extends SelectorThread> nextThreadIterator;

    public <T extends SelectorThread> SelectorThreadLoadBalancer(Collection<T> threads) {
      if (threads.isEmpty()) {
        throw new IllegalArgumentException("At least one selector thread is required");
      }
      this.threads = Collections.unmodifiableList(new ArrayList<T>(threads));
      nextThreadIterator = this.threads.iterator();
    }

    public SelectorThread nextThread() {
      // Choose a selector thread (round robin)
      if (!nextThreadIterator.hasNext()) {
        nextThreadIterator = threads.iterator();
      }
      return nextThreadIterator.next();
    }
  }
}
