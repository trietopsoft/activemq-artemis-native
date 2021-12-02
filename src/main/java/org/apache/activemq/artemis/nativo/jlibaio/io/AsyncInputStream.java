package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jctools.queues.SpscArrayQueue;

public class AsyncInputStream extends InputStream {

    static final Log LOG = LogFactory.getLog(AsyncInputStream.class);

    protected final AsyncStreamContext async;

    protected final int readAhead;

    protected final ReadBuffer[] buffers;

    protected final AtomicInteger pending = new AtomicInteger(0);

    protected final PriorityBlockingQueue<ReadBuffer> readyQueue;

    protected final Queue<ReadBuffer> availableQueue;

    protected ReadBuffer stream;

    protected long position = 0;

    protected long watermark = 0;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 
     * @param async
     * @param readAhead
     * @throws IOException
     */
    public AsyncInputStream(AsyncStreamContext async, int readAhead) throws IOException {
        this.async = async;
        this.readAhead = readAhead;

        this.readyQueue = new PriorityBlockingQueue<>(async.queueSize);
        this.availableQueue = new SpscArrayQueue<>(async.queueSize);

        // Pre-allocate buffer cache
        buffers = new ReadBuffer[async.queueSize];
        IntStream.range(0, async.queueSize).forEach(idx -> {
            buffers[idx] = new ReadBuffer(idx, async.newBuffer(), this.readyQueue);
            this.availableQueue.offer(buffers[idx]);
        });

        // Issue read requests and wait for beginning of stream
        readRequest();
        this.stream = waitStream(0);

    }

    /**
     * Get the aligned block for the specified position
     * 
     * @param position the position
     * @return the block number
     */
    long block(long position) {
        // Buffer size defines block number
        return position / async.bufferSize;
    }

    /**
     * Compute
     * 
     * @param position
     * @return
     */
    long watermark(long position) {
        // Block defines buffer alignment
        return block(position) * async.bufferSize;
    }

    /**
     * 
     * @throws IOException
     */
    protected void readRequest() throws IOException {
        int inFlight = pending.get() + readyQueue.size();
        if (inFlight < readAhead) {
            long blocks = Math.min(readAhead - inFlight, buffers.length);
            for (long i = 0; i < blocks; i++) {
                long req = watermark;
                if (req > async.length) {
                    break;
                }
                ReadBuffer rb = availableQueue.poll();
                if (rb != null) {
                    watermark += this.async.bufferSize;
                    rb.reset(req);
                    rb.active();
                    int len = (int) Math.min(rb.buffer.capacity(), async.length - req);
                    if (async.directio) {
                        // Use block size for compatiblity and fix up after read
                        len = async.blockSize;
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Read request position=" + req + " len=" + len + ", wm=" + watermark);
                    }
                    this.async.fileDescriptor.read(req, len, rb.buffer, rb);
                    pending.incrementAndGet();
                } else {
                    break;
                }
            }
        }
    }

    /**
     * 
     * @param stream
     * @throws IOException
     */
    protected void recycle(ReadBuffer stream) throws IOException {
        if (stream != null) {
            if (closed.get()) {
                stream.close();
            } else {
                this.availableQueue.offer(stream);
            }
        }
        this.stream = null;
    }

    /**
     * 
     * @param position
     * @return
     * @throws IOException
     */
    protected ReadBuffer waitStream(long position) throws IOException {
        if (position > async.length) {
            return null;
        }
        while (!closed.get()) {
            try {
                // Issue pending reads
                readRequest();
                // Check result queue
                ReadBuffer rb = this.readyQueue.poll(1, TimeUnit.SECONDS);
                if (rb == null) {
                    LOG.info("Waiting for read pos=" + position + " pending=" + this.pending.get() + " available="
                            + this.availableQueue.size() + " ready=" + readyQueue.size());
                    continue;
                }
                pending.decrementAndGet();
                if (rb.position == position) {
                    if ((rb.position + async.bufferSize) > async.length) {
                        long lim = async.length - rb.position;
                        rb.buffer.limit((int) lim);
                    }
                    return rb;
                } else if (rb.position > position) {
                    // LOG.info("Out of order read pos=" + position + " buf=" + rb.position + ",
                    // wm=" + watermark);
                    if (rb.position < watermark) {
                        this.readyQueue.offer(rb);
                    } else {
                        LOG.info("Discard far read pos=" + position + " buf=" + rb.position + ", wm=" + watermark);
                        recycle(rb);
                    }
                } else {
                    LOG.info("Discard near read pos=" + position + " buf=" + rb.position + ", wm=" + watermark);
                    recycle(rb);
                }

            } catch (InterruptedException iex) {
                LOG.error("Interrupted?!", iex);
            }
        }
        return null;
    }

    /**
     * 
     */
    @Override
    public int available() throws IOException {
        return (int) Math.min(async.length - position, Integer.MAX_VALUE);
    }

    /**
     * 
     * @return
     */
    public long position() {
        return this.position;
    }

    /**
     * 
     */
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            recycle(this.stream);

            int inFlight = pending.get() + this.readyQueue.size();
            boolean waiters = inFlight > 0;
            if (availableQueue.size() == buffers.length) {
                waiters = false;
            }
            availableQueue.stream().forEach(s -> {
                try {
                    s.close();
                } catch (IOException iox) {
                    LOG.error("Error closing stream buffer", iox);
                }
            });
            if (waiters) {
                LOG.debug("Closing " + inFlight + " in-flight readers");
                while (pending.get() > 0 || this.readyQueue.size() > 0) {
                    try {
                        ReadBuffer rb = this.readyQueue.poll(1, TimeUnit.SECONDS);
                        if (rb != null) {
                            rb.close();
                            pending.decrementAndGet();
                        }
                    } catch (IOException iox) {
                        LOG.error("Error closing buffer", iox);
                    } catch (InterruptedException iex) {
                        LOG.error("Error waiting for ready", iex);
                    }
                }
            }
            Arrays.stream(this.buffers).filter(b -> b.isOpen())
                    .forEach(rb -> LOG.warn("Read buffer remains open: " + rb));
            async.close();
        }
    }

    /** 
     * 
     */
    @Override
    public int read() throws IOException {
        if (position >= async.length) {
            recycle(this.stream);
            return -1;
        }
        if (this.stream.buffer.remaining() == 0) {
            recycle(this.stream);
            if (position < async.length) {
                this.stream = waitStream(position);
            }
        }
        if (this.stream != null) {
            ++position;
            return this.stream.buffer.get();
        } else {
            return -1;
        }
    }

    /**
     * 
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= async.length) {
            recycle(this.stream);
            return -1;
        }
        if (this.stream.buffer.remaining() == 0) {
            recycle(this.stream);
            this.stream = waitStream(position);
        }
        // TODO stream is null?
        // LOG.info("Read buffer " + stream.buffer + " b=" + b.length + " o=" + off + "
        // l=" + len);
        int rem = this.stream.buffer.remaining();
        int read = 0;
        while (position < async.length && len > 0) {
            // LOG.info("Buffer remaining read=" + rem);
            int part = Math.min(len, rem);
            if (part > 0) {
                this.stream.buffer.get(b, off, part);
                position += part;
                rem -= part;
                read += part;

                // Advance the local buffer pointers
                len -= part;
                off += part;
            }
            if (rem == 0) {
                recycle(this.stream);
                if (position < async.length) {
                    this.stream = waitStream(position);
                }
                if (this.stream != null) {
                    rem = this.stream.buffer.remaining();
                } else {
                    break;
                }
            }
        }
        return read;
    }

    /**
     * 
     */
    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        return seek(this.position + n);
    }

    /**
     * 
     * @param p
     * @return
     * @throws IOException
     */
    public long seek(long p) throws IOException {
        // Find delta
        long delta = p - this.position;
        if (p < 0 || delta == 0 || p > this.async.file.length()) {
            return 0;
        } else {
            // Able to fast seek?
            if (LOG.isTraceEnabled()) {
                LOG.trace("Seek: " + stream + " p=" + position + " d=" + delta);
            }
            if (this.stream != null && ((delta > 0 && this.stream.buffer.remaining() >= delta)
                    || (delta < 0 && this.stream.buffer.position() >= Math.abs(delta)))) {
                this.stream.buffer.position(this.stream.buffer.position() + (int) delta);
                this.position += delta;
                return delta;
            }
            // Return current stream to available queue
            recycle(this.stream);
            // Beyond block boundary
            long newPosition = this.position + delta;
            // Update watermark, save wm before read
            long wm = watermark(newPosition);
            this.watermark = wm;
            // Wait for watermark position, will increment watermark
            this.stream = waitStream(this.watermark);
            // Fast forward current buffer
            this.stream.buffer.position(this.stream.buffer.position() + (int) (newPosition - wm));
            this.position = newPosition;
        }
        return delta;
    }

}
