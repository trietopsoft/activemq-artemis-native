package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WriteBuffer extends StreamBuffer {

    static final Log LOG = LogFactory.getLog(WriteBuffer.class);

    protected final AsyncOutputStream out;

    protected int commit = 0;

    protected WriteBuffer(int id, ByteBuffer buf, AsyncOutputStream out) {
        super(id, buf);
        this.out = out;
    }

    public void restore() {
        this.commit = buffer.limit();
        buffer.position(this.commit);
        buffer.limit(buffer.capacity());
    }

    public boolean isDirty() {
        return this.commit < buffer.position();
    }

    @Override
    public void error() {
        LOG.info("ERR: " + this);
        if (this.partial) {
            restore();
            if (this.latch != null) {
                this.latch.countDown();
            }
            LOG.info("Error w/ Flush " + id);
        }
        // Put myself back in the cache
        else if (this.buffer != null) {
            if (this.active.compareAndSet(true, false)) {
                this.out.offer(this);
            } else {
                LOG.error("Error called after reset " + id);
            }
        } else {
            LOG.error("Error after close " + id);
        }
    }

    @Override
    public void done() {
        // Pass thru error handling
        if (isError()) {
            return;
        }
        if (this.buffer != null) {
            if (this.partial) {
                restore();
                if (this.latch != null) {
                    this.latch.countDown();
                }
            }
            // Put myself back in the cache
            else if (this.active.compareAndSet(true, false)) {
                reset(-1);
                this.out.offer(this);
            } else {
                LOG.error("Done called after reset " + id);
            }
        } else {
            LOG.error("Done after close " + id);
        }
    }

    @Override
    public String toString() {
        return "WriteBuffer [id=" + id + ", active=" + active + ", position=" + position + ", buffer=" + buffer
                + ", err=" + this.errNo + "]";
    }

}
