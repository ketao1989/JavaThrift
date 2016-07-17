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

import org.apache.thrift.TAsyncProcessor;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 为非阻塞TServer实现提供公共的方法和类
 */
public abstract class AbstractNonblockingServer extends TServer {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

  public static abstract class AbstractNonblockingServerArgs<T extends AbstractNonblockingServerArgs<T>> extends AbstractServerArgs<T> {
    public long maxReadBufferBytes = Long.MAX_VALUE;

    public AbstractNonblockingServerArgs(TNonblockingServerTransport transport) {
      super(transport);
      transportFactory(new TFramedTransport.Factory());//设置输入输出传输协议
    }
  }

  /**
   * The maximum amount of memory we will allocate to client IO buffers at a
   * time. Without this limit, the server will gladly allocate client buffers
   * right into an out of memory exception, rather than waiting.
   */
  final long MAX_READ_BUFFER_BYTES;//一次分配给IO缓存的最大数量;不设置size,则会一直分配内存直到内存异常

  /**
   * 当前已经分配给read 缓存 多少字节
   */
  final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

  public AbstractNonblockingServer(AbstractNonblockingServerArgs args) {
    super(args);
    MAX_READ_BUFFER_BYTES = args.maxReadBufferBytes;
  }

  /**
   * Begin accepting connections and processing invocations.
   */
  public void serve() {
    // 1. 启动各个IO线程
    if (!startThreads()) {
      return;
    }

    // 2.启动socket 监听listening
    if (!startListening()) {
      return;
    }

    // 3. 设置服务可用
    setServing(true);

    // 4. block 直到结束
    waitForShutdown();

    // 5. 设置服务不可用
    setServing(false);

    // 6. 停止socket,关闭监听listening
    stopListening();
  }

  /**
   * Starts any threads required for serving.
   * 
   * @return true if everything went ok, false if threads could not be started.
   */
  protected abstract boolean startThreads();

  /**
   * A method that will block until when threads handling the serving have been
   * shut down.
   */
  protected abstract void waitForShutdown();

  /**
   * Have the server transport start accepting connections.
   * 
   * @return true if we started listening successfully, false if something went
   *         wrong.
   */
  protected boolean startListening() {
    try {
      serverTransport_.listen();
      return true;
    } catch (TTransportException ttx) {
      LOGGER.error("Failed to start listening on server socket!", ttx);
      return false;
    }
  }

  /**
   * Stop listening for connections.
   */
  protected void stopListening() {
    serverTransport_.close();
  }

  /**
   * 执行一个调用.这个方法表现为多个不同方式--直接调用,队列分离执行等.
   *
   * Perform an invocation. This method could behave several different ways -
   * invoke immediately inline, queue for separate execution, etc.
   * 
   * @return true if invocation was successfully requested, which is not a
   *         guarantee that invocation has completed. False if the request
   *         failed.
   */
  protected abstract boolean requestInvoke(FrameBuffer frameBuffer);

  /**
   * An abstract thread that handles selecting on a set of transports and
   * {@link FrameBuffer FrameBuffers} associated with selected keys
   * corresponding to requests.
   */
  protected abstract class AbstractSelectThread extends Thread {

    protected final Selector selector;//selector nio

    // List of FrameBuffers that want to change their selection interests.
    // 需要更改selection interest 的FrameBuffers列表(FrameBuffers包括所有通信信息)
    protected final Set<FrameBuffer> selectInterestChanges = new HashSet<FrameBuffer>();

    public AbstractSelectThread() throws IOException {
      this.selector = SelectorProvider.provider().openSelector();//开启一个nio selector
    }

    /**
     * If the selector is blocked, wake it up.
     */
    public void wakeupSelector() {
      selector.wakeup();
    }

    /**
     * Add FrameBuffer to the list of select interest changes and wake up the
     * selector if it's blocked. When the select() call exits, it'll give the
     * FrameBuffer a chance to change its interests.
     */
    public void requestSelectInterestChange(FrameBuffer frameBuffer) {
      synchronized (selectInterestChanges) {
        selectInterestChanges.add(frameBuffer);
      }
      // wakeup the selector, if it's currently blocked.
      selector.wakeup(); // 唤醒对应selector
    }

    /**
     * 更改 selection interest key 从 read 到 write 模式.
     *
     * Check to see if there are any FrameBuffers that have switched their
     * interest type from read to write or vice versa.
     */
    protected void processInterestChanges() {
      synchronized (selectInterestChanges) {
        for (FrameBuffer fb : selectInterestChanges) {
          fb.changeSelectInterests();
        }
        selectInterestChanges.clear();
      }
    }

    /**
     * Do the work required to read from a readable client. If the frame is
     * fully read, then invoke the method call.
     */
    protected void handleRead(SelectionKey key) {
      FrameBuffer buffer = (FrameBuffer) key.attachment();
      if (!buffer.read()) { //如果读取失败,则清理该可key对应的buffer
        cleanupSelectionKey(key);
        return;
      }

      // if the buffer's frame read is complete, invoke the method.
      if (buffer.isFrameFullyRead()) { // 内容读取完成,则调用具体的方法处理buffer
        if (!requestInvoke(buffer)) { // 具体的操作由子类去实现,默认buffer.invoke()直接执行
          cleanupSelectionKey(key); // 执行失败,清理buffer
        }
      }
    }

    /**
     * Let a writable client get written, if there's data to be written.
     */
    protected void handleWrite(SelectionKey key) {
      FrameBuffer buffer = (FrameBuffer) key.attachment();
      if (!buffer.write()) {
        cleanupSelectionKey(key);
      }
    }

    /**
     * 关闭 buffer(关闭连接,重置已分配size,调用自定义方法),取消注册的selection key.
     */
    protected void cleanupSelectionKey(SelectionKey key) {
      // remove the records from the two maps
      FrameBuffer buffer = (FrameBuffer) key.attachment();
      if (buffer != null) {
        // close the buffer
        buffer.close();
      }
      // cancel the selection key
      key.cancel();
    }
  } // SelectThread

  /**
   * FrameBuffer 状态机可能的状态
   */
  private enum FrameBufferState {
    // in the midst of reading the frame size off the wire
    READING_FRAME_SIZE,//正在读取数据大小
    // reading the actual frame data now, but not all the way done yet
    READING_FRAME,//这个在读取数据
    // completely read the frame, so an invocation can now happen
    READ_FRAME_COMPLETE,//读取数据完成
    // waiting to get switched to listening for write events
    AWAITING_REGISTER_WRITE,//等待切换到write时间监听模式
    // started writing response data, not fully complete yet
    WRITING,//正在write响应数据
    // another thread wants this framebuffer to go back to reading
    AWAITING_REGISTER_READ,//等待读取数据
    // we want our transport and selection key invalidated in the selector
    // thread
    AWAITING_CLOSE // 等待关闭
  }

  /**
   * 在一个client和一个invoker交易之间,该类实现了一个有序的状态机.它主要涉及 读frame size 和数据,
   * 从对应处理的封装的transport中获取;然后写回相应数据给client.在处理过程中,它管理了基于selection key
   * 之上的读写切换.
   *
   * Class that implements a sort of state machine around the interaction with a
   * client and an invoker. It manages reading the frame size and frame data,
   * getting it handed off as wrapped transports, and then the writing of
   * response data back to the client. In the process it manages flipping the
   * read and write bits on the selection key for its client.
   */
   public class FrameBuffer {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    // the actual transport hooked up to the client.
    protected final TNonblockingTransport trans_;

    // the SelectionKey that corresponds to our transport
    protected final SelectionKey selectionKey_;

    // the SelectThread that owns the registration of our transport
    protected final AbstractSelectThread selectThread_;

    // where in the process of reading/writing are we?
    // 处理的buffer状态,默认为正在读frame size.
    protected FrameBufferState state_ = FrameBufferState.READING_FRAME_SIZE;

    // the ByteBuffer we'll be using to write and read, depending on the state
    protected ByteBuffer buffer_;

    // 响应的数据流
    protected final TByteArrayOutputStream response_;
    
    // the frame that the TTransport should wrap.
    protected final TMemoryInputTransport frameTrans_;
    
    // 连接client的输入输出transport
    protected final TTransport inTrans_;
    
    protected final TTransport outTrans_;
    
    // the input protocol to use on frames
    protected final TProtocol inProt_;
    
    // the output protocol to use on frames
    protected final TProtocol outProt_;
    
    // 关联到connection的context
    protected final ServerContext context_;

    // 初始化 framebuffer
    public FrameBuffer(final TNonblockingTransport trans,
        final SelectionKey selectionKey,
        final AbstractSelectThread selectThread) {
      trans_ = trans;
      selectionKey_ = selectionKey;
      selectThread_ = selectThread;
      buffer_ = ByteBuffer.allocate(4);

      frameTrans_ = new TMemoryInputTransport();
      response_ = new TByteArrayOutputStream();
      inTrans_ = inputTransportFactory_.getTransport(frameTrans_);
      outTrans_ = outputTransportFactory_.getTransport(new TIOStreamTransport(response_));
      inProt_ = inputProtocolFactory_.getProtocol(inTrans_);
      outProt_ = outputProtocolFactory_.getProtocol(outTrans_);

      if (eventHandler_ != null) {
        context_ = eventHandler_.createContext(inProt_, outProt_);//设置用户自定义的上下文
      } else {
        context_  = null;
      }
    }

    /**
     * TODO 读取数据
     * Give this FrameBuffer a chance to read. The selector loop should have
     * received a read event for this FrameBuffer.
     * 
     * @return true if the connection should live on, false if it should be
     *         closed
     */
    public boolean read() {
      if (state_ == FrameBufferState.READING_FRAME_SIZE) {
        // try to read the frame size completely
        if (!internalRead()) { // 将数据读取到buffer中
          return false;
        }

        // if the frame size has been read completely, then prepare to read the
        // actual frame.
        if (buffer_.remaining() == 0) { //如果buffer中有数据
          // pull out the frame size as an integer.
          int frameSize = buffer_.getInt(0); // 获取buffer数据的size
          if (frameSize <= 0) {
            LOGGER.error("Read an invalid frame size of " + frameSize
                + ". Are you using TFramedTransport on the client side?");
            return false;
          }

          // if this frame will always be too large for this server, log the
          // error and close the connection.
          if (frameSize > MAX_READ_BUFFER_BYTES) {
            LOGGER.error("Read a frame size of " + frameSize
                + ", which is bigger than the maximum allowable buffer size for ALL connections.");
            return false;
          }

          // if this frame will push us over the memory limit, then return.
          // with luck, more memory will free up the next time around.
          // 如果获取的数据超过了内存的限制,则返回.如果幸运,接下来更多地内存空间会被释放,从而下次可以读取成功.
          if (readBufferBytesAllocated.get() + frameSize > MAX_READ_BUFFER_BYTES) {
            return true;
          }

          // increment the amount of memory allocated to read buffers
          // 增长已分配的内存空间来读取buffer,[size head] + [frame data] = 4 + frameSize
          readBufferBytesAllocated.addAndGet(frameSize + 4);

          // reallocate the readbuffer as a frame-sized buffer
          buffer_ = ByteBuffer.allocate(frameSize + 4);
          buffer_.putInt(frameSize);

          state_ = FrameBufferState.READING_FRAME; // size 读取完了,接下来读取data
        } else {
          // this skips the check of READING_FRAME state below, since we can't
          // possibly go on to that state if there's data left to be read at
          // this one.
          return true;
        }
      }

      // it is possible to fall through from the READING_FRAME_SIZE section
      // to READING_FRAME if there's already some frame data available once
      // READING_FRAME_SIZE is complete.

      if (state_ == FrameBufferState.READING_FRAME) {
        if (!internalRead()) { // 继续从transport中获取数据,可能有些数据上次size没有获取完
          return false;
        }

        // since we're already in the select loop here for sure, we can just
        // modify our selection key directly.
        if (buffer_.remaining() == 0) { // 没有数据空间可写到buffer了,
          // get rid of the read select interests
          selectionKey_.interestOps(0);//重置selection key
          state_ = FrameBufferState.READ_FRAME_COMPLETE;//读完成状态
        }

        return true;
      }

      // if we fall through to this point, then the state must be invalid.
      LOGGER.error("Read was called but state is invalid (" + state_ + ")");
      return false;
    }

    /**
     * Give this FrameBuffer a chance to write its output to the final client.
     */
    public boolean write() {
      if (state_ == FrameBufferState.WRITING) {
        try {
          if (trans_.write(buffer_) < 0) {//buffer数据写到transport socket上
            return false;
          }
        } catch (IOException e) {
          LOGGER.warn("Got an IOException during write!", e);
          return false;
        }

        // we're done writing. now we need to switch back to reading.
        if (buffer_.remaining() == 0) {//buffer没有数据可写到transport了,切换到读模式
          prepareRead();
        }
        return true;
      }

      LOGGER.error("Write was called, but state is invalid (" + state_ + ")");
      return false;
    }

    /**
     * 将FrameBuffer 的interest selectionKey 设置为 write,当数据进来的时候.
     *
     * Give this FrameBuffer a chance to set its interest to write, once data
     * has come in.
     */
    public void changeSelectInterests() {
      if (state_ == FrameBufferState.AWAITING_REGISTER_WRITE) {
        // set the OP_WRITE interest
        selectionKey_.interestOps(SelectionKey.OP_WRITE);
        state_ = FrameBufferState.WRITING;
      } else if (state_ == FrameBufferState.AWAITING_REGISTER_READ) {
        prepareRead();
      } else if (state_ == FrameBufferState.AWAITING_CLOSE) {
        close();
        selectionKey_.cancel();
      } else {
        LOGGER.error("changeSelectInterest was called, but state is invalid (" + state_ + ")");
      }
    }

    /**
     * 关闭连接connection
     */
    public void close() {
      // if we're being closed due to an error, we might have allocated a
      // buffer that we need to subtract for our memory accounting.

      // 当我们由于error关闭连接,则需要将内存使用量减掉改buffer已被分配的空间大小.
      if (state_ == FrameBufferState.READING_FRAME || 
          state_ == FrameBufferState.READ_FRAME_COMPLETE ||
          state_ == FrameBufferState.AWAITING_CLOSE) {
        readBufferBytesAllocated.addAndGet(-buffer_.array().length);
      }
      trans_.close(); // 关闭socket连接
      if (eventHandler_ != null) {
        eventHandler_.deleteContext(context_, inProt_, outProt_); // 调用业务的清理方法
      }
    }

    /**
     * Check if this FrameBuffer has a full frame read.
     */
    public boolean isFrameFullyRead() {
      return state_ == FrameBufferState.READ_FRAME_COMPLETE;
    }

    /**
     * 在处理器执行完之后,线程在调用方法执行完了之后需要调用改responseReady方法.
     * 如果,在 响应buffer里没有任务市局,则尝试reading数据.
     *
     * After the processor has processed the invocation, whatever thread is
     * managing invocations should call this method on this FrameBuffer so we
     * know it's time to start trying to write again. Also, if it turns out that
     * there actually isn't any data in the response buffer, we'll skip trying
     * to write and instead go back to reading.
     */
    public void responseReady() {
      // the read buffer is definitely no longer in use, so we will decrement
      // our read buffer count. we do this here as well as in close because
      // we'd like to free this read memory up as quickly as possible for other
      // clients.
      // read 的数据不再需要,可以将已分配大小减小
      readBufferBytesAllocated.addAndGet(-buffer_.array().length);

      if (response_.len() == 0) {
        // go straight to reading again. this was probably an oneway method
        state_ = FrameBufferState.AWAITING_REGISTER_READ;//直接去读数据
        buffer_ = null;
      } else {
        buffer_ = ByteBuffer.wrap(response_.get(), 0, response_.len());//将响应数据写入buffer

        // set state that we're waiting to be switched to write. we do this
        // asynchronously through requestSelectInterestChange() because there is
        // a possibility that we're not in the main thread, and thus currently
        // blocked in select(). (this functionality is in place for the sake of
        // the HsHa server.)
        state_ = FrameBufferState.AWAITING_REGISTER_WRITE;//切换到等待写状态
      }
      requestSelectInterestChange();
    }

    /**
     * Actually invoke the method signified by this FrameBuffer.
     * buffer实际调用方法.
     * TODO 由于可能时线程池调用,所以在执行完成之后,需要主动调用responseReady返回响应数据
     */
    public void invoke() {
      frameTrans_.reset(buffer_.array());//重置pos
      response_.reset();//初始化
      
      try {
        if (eventHandler_ != null) {
          eventHandler_.processContext(context_, inTrans_, outTrans_);//调用自定义方法
        }
        // 调用 thrift生成的java代码,然后调用实际的实现
        processorFactory_.getProcessor(inTrans_).process(inProt_, outProt_);//默认new TProcessorFactory(processor)
        // 调用write操作返回数据
        responseReady();
        return;
      } catch (TException te) {
        LOGGER.warn("Exception while invoking!", te);
      } catch (Throwable t) {
        LOGGER.error("Unexpected throwable while invoking!", t);
      }
      // This will only be reached when there is a throwable.
      state_ = FrameBufferState.AWAITING_CLOSE;//异常关闭
      requestSelectInterestChange();//切换到read模式
    }

    /**
     * Perform a read into buffer.
     * 
     * @return true if the read succeeded, false if there was an error or the
     *         connection closed.
     */
    private boolean internalRead() {
      try {
        if (trans_.read(buffer_) < 0) {
          return false;
        }
        return true;
      } catch (IOException e) {
        LOGGER.warn("Got an IOException in internalRead!", e);
        return false;
      }
    }

    /**
     * We're done writing, so reset our interest ops and change state
     * accordingly.
     */
    private void prepareRead() {
      // we can set our interest directly without using the queue because
      // we're in the select thread.
      selectionKey_.interestOps(SelectionKey.OP_READ);
      // get ready for another go-around
      buffer_ = ByteBuffer.allocate(4);
      state_ = FrameBufferState.READING_FRAME_SIZE;
    }

    /**
     * 当 FrameBuffer 需要改变它的select interest和可能不再它select线程的执行方法,然后
     * 这个方法确保当对应的select线程唤醒之后,interest改变会完成.当当前线程时这个FrameBuffer的select线程,
     * 则直接改变interest.
     */
    protected void requestSelectInterestChange() {
      if (Thread.currentThread() == this.selectThread_) {
        changeSelectInterests();
      } else {
        this.selectThread_.requestSelectInterestChange(this);//唤醒selector
      }
    }
  } // FrameBuffer

  public class AsyncFrameBuffer extends FrameBuffer {
    public AsyncFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey, AbstractSelectThread selectThread) {
      super(trans, selectionKey, selectThread);
    }

    public TProtocol getInputProtocol() {
      return  inProt_;
    }

    public TProtocol getOutputProtocol() {
      return outProt_;
    }


    public void invoke() {
      frameTrans_.reset(buffer_.array());
      response_.reset();

      try {
        if (eventHandler_ != null) {
          eventHandler_.processContext(context_, inTrans_, outTrans_);
        }
        ((TAsyncProcessor)processorFactory_.getProcessor(inTrans_)).process(this);// 这里使用的asyncProcessor
        return;
      } catch (TException te) {
        LOGGER.warn("Exception while invoking!", te);
      } catch (Throwable t) {
        LOGGER.error("Unexpected throwable while invoking!", t);
      }
      // This will only be reached when there is a throwable.
      state_ = FrameBufferState.AWAITING_CLOSE;
      requestSelectInterestChange();
    }
  }
}
