package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.nativo.jlibaio.SubmitInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class StreamBuffer implements SubmitInfo, Closeable {

    static final Log LOG = LogFactory.getLog(StreamBuffer.class);

    protected final int id;

    protected final AtomicBoolean active = new AtomicBoolean(false);

    protected CountDownLatch latch;

    protected boolean partial;

    protected ByteBuffer buffer;

    protected int errNo;

    protected String errMsg;

    protected long position = 0;

    protected StreamBuffer(int id, ByteBuffer buf) {
        this.id = id;
        this.buffer = buf;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setPartial(boolean partial) {
        this.partial = partial;
    }

    public boolean isPartial() {
        return this.partial;
    }

    public void active() {
        if (!this.active.compareAndSet(false, true)) {
            LOG.error("Active flag not false for " + id);
        }
    }

    protected void reset(long position) {
        this.errNo = 0;
        this.errMsg = null;
        this.position = position;
        this.buffer.clear();
        this.latch = null;
        this.partial = false;
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            LibaioContext.freeBuffer(buffer);
            buffer = null;
        }
    }

    public boolean isOpen() {
        return this.buffer != null;
    }

    public String errorMessage() {
        return errNo + ": " + errMsg;
    }

    public boolean isError() {
        return this.errNo != 0;
    }

    public abstract void error();

    @Override
    public void onError(int errno, String message) {
        try {
            throw new IOException(errno + ": " + message);
        } catch (IOException iox) {
            LOG.error("on error", iox);
        }
        this.errMsg = message;
        this.errNo = errno;
        LOG.error("Callback error: " + errorMessage());
        error();
    }

}
