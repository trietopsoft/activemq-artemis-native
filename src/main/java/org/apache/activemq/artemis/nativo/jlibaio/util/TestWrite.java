package org.apache.activemq.artemis.nativo.jlibaio.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.nativo.jlibaio.io.AsyncOutputStream;
import org.apache.activemq.artemis.nativo.jlibaio.io.AsyncStreamContext;

public class TestWrite {

    static final int BLOCKS = 128;

    public static void main(String[] args) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        for (int i = 0; i < 4096; i++) {
            buffer.put((byte) 'a');
        }
        buffer.rewind();
        byte[] data = buffer.array();

        final long blocks = 4096 * BLOCKS;

        File outFile = new File(args[0]);

        long start = System.currentTimeMillis();
        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(outFile).queueSize(5).blocks(32)
                .useFdataSync(false).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos, 8192)) {
            for (int i = 0; i < blocks; i++) {
                bos.write(data);
            }
        }
        long stop = System.currentTimeMillis();

        System.out.println("AIO write elapsed: " + (stop - start) + " bytes=" + outFile.length());
    }
}
