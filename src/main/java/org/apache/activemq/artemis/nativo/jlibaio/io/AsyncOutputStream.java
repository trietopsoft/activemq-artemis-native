package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsyncOutputStream extends OutputStream {

    static final Log LOG = LogFactory.getLog(AsyncOutputStream.class);

    protected final AsyncStreamContext async;

    protected final WriteBuffer[] buffers;

    protected final BlockingQueue<WriteBuffer> bufferCache;

    protected WriteBuffer stream;

    protected long position = 0;

    protected AtomicBoolean closed = new AtomicBoolean();

    protected AsyncOutputStream(AsyncStreamContext async, boolean append, long preallocate) throws IOException {
        this.async = async;

        // TODO should queueSize be something else
        // this.bufferCache = new MpscBlockingConsumerArrayQueue<>(async.queueSize);
        this.bufferCache = new ArrayBlockingQueue<>(async.queueSize);
        // Pre-allocate buffer cache
        buffers = new WriteBuffer[async.queueSize];
        IntStream.range(0, async.queueSize).forEach(idx -> {
            buffers[idx] = new WriteBuffer(idx, async.newBuffer(), this);
            offer(buffers[idx]);
        });

        if (append) {
            SynchronousQueue<ReadBuffer> sq = new SynchronousQueue<>();
            try (ReadBuffer reader = new ReadBuffer(0, async.newBuffer(), sq)) {
                long pos = async.blockSize * (async.length / async.blockSize);
                if (pos < async.length) {
                    int len = (int) (async.length - pos);
                    if (async.directio) {
                        len = async.blockSize;
                    }
                    reader.active();
                    async.fileDescriptor.read(pos, len, reader.buffer, reader);
                    // Wait here, return val is same instance as 'reader'
                    sq.take();
                    this.stream = getBuffer(pos);
                    this.stream.buffer.put(reader.buffer);
                    this.stream.buffer.position((int) (async.length - pos));
                    this.position = async.length;
                } else {
                    this.stream = getBuffer(pos);
                }
            } catch (InterruptedException iex) {
                throw new IOException(iex.getMessage());
            }
        } else {
            this.stream = getBuffer(position);
            if (preallocate > 0) {
                long start = System.currentTimeMillis();
                this.async.fileDescriptor.fallocate(preallocate);
                long stop = System.currentTimeMillis();
                LOG.trace(
                        "Pre-allocated space: " + async.file + " bytes: " + preallocate + " " + (stop - start) + "ms");
            }
        }
    }

    void offer(WriteBuffer stream) {
        this.bufferCache.offer(stream);
    }

    protected WriteBuffer getBuffer(long position) throws IOException {
        WriteBuffer buf = null;
        while (!this.closed.get() && buf == null && position >= 0) {
            // Get write buffer
            try {
                buf = this.bufferCache.poll(100, TimeUnit.MILLISECONDS);
                if (buf == null) {
                    continue;
                }
                if (buf.isError()) {
                    LOG.error("Error noticed @ get " + buf.errorMessage());
                    // Forward buffer through to close
                    this.bufferCache.offer(buf);
                    throw new IOException(buf.errorMessage());
                }
                buf.reset(position);
                buf.active();

                return buf;
            } catch (InterruptedException iex) {
                return null;
            }
        }
        throw new IOException("Unable to obtain write buffer at position " + position);
    }

    protected WriteBuffer flushBuffer(WriteBuffer stream, boolean getNext) throws IOException {
        int len = stream.buffer.position();
        if (len > 0) {
            stream.buffer.flip();
            if (async.directio && len != stream.buffer.capacity()) {
                // Direct IO requires a multiple of a "block" of data
                int blks = 1 + (len / async.blockSize);
                len = blks * async.blockSize;
                stream.setPartial(true);
            }
            LOG.debug("Writing: " + len + " bytes at " + stream.position + " with " + stream);
            async.fileDescriptor.write(stream.position, len, stream.buffer, stream);
        } else {
            offer(stream);
        }
        if (getNext) {
            return getBuffer(position);
        } else {
            if (this.stream.isPartial()) {
                return stream;
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (this.stream != null && this.stream.isDirty()) {
                flush();
            }
            // Flush may alter stream state
            if (this.stream != null) {
                this.bufferCache.offer(this.stream);
                this.stream = null;
            }

            int toClose = buffers.length;
            int closed = 0;
            String errMsg = null;
            while (toClose > 0) {
                try {
                    WriteBuffer buf = this.bufferCache.poll(1, TimeUnit.SECONDS);
                    if (buf != null) {
                        if (buf.isError()) {
                            LOG.error("Error noticed @ close " + buf.errorMessage());
                            errMsg = buf.errorMessage();
                        }
                        buf.close();
                        toClose--;
                        LOG.debug("Closed " + buf + " rem: " + toClose + " closed: " + (++closed));
                    } else {
                        LOG.info("Waiting for: "
                                + Arrays.asList(buffers).stream().filter(b -> b.isOpen()).collect(Collectors.toList()));
                    }
                } catch (InterruptedException iex) {
                    // TODO handle interruption
                    LOG.error("Interrupted?!", iex);
                }
            }
            Arrays.stream(this.buffers).filter(b -> b.isOpen())
                    .forEach(wb -> LOG.warn("Write buffer remains open: " + wb));
            if (async.syncOnClose) {
                async.fileDescriptor.sync();
            }
            if (async.directio && (this.position % async.blockSize) != 0) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Truncating file to " + position);
                }
                async.fileDescriptor.truncate(this.position);
            }

            async.close();
            if (errMsg != null) {
                throw new IOException(errMsg);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        this.stream.setLatch(latch);
        this.stream = flushBuffer(this.stream, false);
        if (this.stream != null) {
            try {
                latch.await();
            } catch (InterruptedException iex) {
                throw new IOException(iex.getMessage());
            }
        } else if (!closed.get()) {
            this.stream = getBuffer(this.position);
        }
    }

    public void sync() throws IOException {
        this.async.fileDescriptor.sync();
    }

    @Override
    public void write(int b) throws IOException {
        if (this.stream.buffer.remaining() == 0) {
            this.stream = flushBuffer(this.stream, true);
        }
        this.stream.buffer.put((byte) (0xFF & b));
        ++position;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int rem = this.stream.buffer.remaining();
        while (len > 0) {
            int part = Math.min(len, rem);
            if (part > 0) {
                this.stream.buffer.put(b, off, part);
                position += part;
                rem -= part;

                // Advance the local buffer pointers
                len -= part;
                off += part;
            }
            if (rem == 0) {
                this.stream = flushBuffer(this.stream, true);
                rem = this.stream.buffer.remaining();
            }
        }
    }

}