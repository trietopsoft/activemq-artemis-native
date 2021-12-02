package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReadBuffer extends StreamBuffer implements Comparable<ReadBuffer> {

    static final Log LOG = LogFactory.getLog(ReadBuffer.class);

    protected final BlockingQueue<ReadBuffer> queue;

    protected ReadBuffer(int id, ByteBuffer buf, BlockingQueue<ReadBuffer> queue) {
        super(id, buf);
        this.queue = queue;
    }

    @Override
    public int compareTo(ReadBuffer o) {
        return position > o.position ? 1 : position < o.position ? -1 : 0;
    }

    @Override
    public void error() {
        LOG.error("ERR " + id + " " + this.errorMessage());
    }

    @Override
    public void done() {
        // Put myself back in the cache
        if (this.buffer != null) {
            if (this.active.compareAndSet(true, false)) {
                // Buffer is already flipped, so add to input stream
                this.queue.offer(this);
                // LOG.error("Done " + id);
            } else {
                LOG.error("Done called after reset " + id);
            }
        } else {
            LOG.error("Done after close " + id);
        }
    }

    @Override
    public String toString() {
        return "ReadBuffer [active=" + active + ", buffer=" + buffer + ", id=" + id + ", position=" + position + "]";
    }

}
