package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsyncStreamContext implements Closeable {

    static final Log LOG = LogFactory.getLog(AsyncStreamContext.class);

    public static final int DEFAULT_QUEUE_SIZE = 50;

    public static final int DEFAULT_BUFFER_SIZE = 128;

    private static AtomicLong id = new AtomicLong();

    public static class Builder {

        private File file;

        private int blocks = DEFAULT_BUFFER_SIZE;

        private int queueSize = DEFAULT_QUEUE_SIZE;

        private boolean append = false;

        private boolean directio = false;

        private boolean fdataSync = false;

        private boolean syncOnClose = true;

        private boolean semaphore = true;

        private int readAhead = 1;

        private long preallocate = 0;

        public Builder file(File file) {
            this.file = file;
            return this;
        }

        public Builder readOnly() {
            this.directio = false;
            this.fdataSync = false;
            this.syncOnClose = false;
            return this;
        }

        public Builder append() {
            return append(true);
        }

        public Builder append(boolean append) {
            this.append = append;
            return this;
        }

        public Builder readAhead(int distance) {
            this.readAhead = distance;
            return this;
        }

        public Builder blocks(int blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder queueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public Builder useDirectIO(boolean direct) {
            this.directio = direct;
            return this;
        }

        public Builder useFdataSync(boolean sync) {
            this.fdataSync = sync;
            return this;
        }

        public Builder syncOnClose(boolean sync) {
            this.syncOnClose = sync;
            return this;
        }

        public Builder useSubmitSemaphore(boolean submitSemaphore) {
            this.semaphore = submitSemaphore;
            return this;
        }

        public Builder preallocate(long len) {
            this.preallocate = len;
            return this;
        }

        public AsyncStreamContext build() throws IOException {
            return new AsyncStreamContext(this);
        }

        public AsyncInputStream inputStream() throws IOException {
            if (readAhead > queueSize) {
                LOG.warn("Read-ahead cannot exceed queueSize=" + queueSize);
                readAhead = queueSize;
            } else if (readAhead <= 0) {
                readAhead = 1;
            }
            return new AsyncInputStream(build(), readAhead);
        }

        public AsyncOutputStream outputStream() throws IOException {
            return new AsyncOutputStream(build(), append, preallocate);
        }

    }

    protected final File file;

    protected final long length;

    protected final int blockSize;

    protected final int bufferSize;

    protected final int queueSize;

    protected final boolean directio;

    protected final LibaioContext<StreamBuffer> context;

    protected final LibaioFile<StreamBuffer> fileDescriptor;

    protected boolean syncOnClose;

    protected AsyncStreamContext(Builder build) throws IOException {
        this.file = build.file;
        this.length = this.file.length();
        this.queueSize = build.queueSize;
        this.syncOnClose = build.syncOnClose;
        this.directio = build.directio;
        this.blockSize = LibaioContext.getBlockSize(this.file.getParentFile());
        if (this.blockSize <= 0) {
            LOG.error("Error obtaining block size from path " + this.file.getParentFile() + ", got " + this.blockSize);
        }
        this.bufferSize = build.blocks * this.blockSize;

        // Stats report
        LOG.debug("File " + file + " block size " + blockSize + ", sync on close? " + syncOnClose);
        LOG.trace("Opening " + queueSize + " with block " + build.blocks + "*" + this.blockSize + " buffers of size "
                + this.bufferSize + ", total mem: " + (((long) this.bufferSize) * this.queueSize) + " bytes");

        this.context = new LibaioContext<>(build.queueSize, build.semaphore, build.fdataSync);
        this.fileDescriptor = this.context.openFile(this.file, this.directio);

        // Start thread poller for context, will exit on close
        Thread t = new Thread() {
            @Override
            public void run() {
                context.poll();
            }
        };
        t.setName("AsyncStream-" + bufferSize + "-" + id.incrementAndGet());

        t.start();
    }

    public ByteBuffer newBuffer() {
        return LibaioContext.newAlignedBuffer(this.bufferSize, this.blockSize);
    }

    @Override
    public void close() throws IOException {
        this.fileDescriptor.close();
        this.context.close();
        LOG.debug("Closed " + this);
    }

}
