/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.nativo.jlibaio.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test is using a different package from {@link LibaioFile} as I need to
 * validate public methods on the API
 */
public class TestAsyncStreams {

    static final Log LOG = LogFactory.getLog(TestAsyncStreams.class);

    @Rule
    public TemporaryFolder temporaryFolder;

    File workingFile;

    public static final long BLOCKS = 1;

    public static final long LEN = 4096 * BLOCKS;

    public void writeFile() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        for (int i = 0; i < 4096; i++) {
            buffer.put((byte) 'a');
        }
        buffer.rewind();
        byte[] data = buffer.array();

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(5).blocks(32)
                .useFdataSync(false).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos, 8192)) {
            for (int i = 0; i < LEN; i++) {
                bos.write(data);
            }
        }
    }

    @Before
    public void createFile() throws IOException {
        workingFile = new File("target/test.bin" + System.currentTimeMillis());
        workingFile.createNewFile();
    }

    @After
    public void deleteFactory() {
        validateLibaio();
    }

    public void validateLibaio() {
        Assert.assertEquals(0, LibaioContext.getTotalMaxIO());
    }

    public TestAsyncStreams() {
        /*
         * I didn't use /tmp for three reasons - Most systems now will use tmpfs which
         * is not compatible with O_DIRECT - This would fill up /tmp in case of
         * failures. - target is cleaned up every time you do a mvn clean, so it's safer
         */
        File parent = new File("./target");
        parent.mkdirs();
        temporaryFolder = new TemporaryFolder(parent);
    }

    @Test
    @Ignore
    public void testStreamOpen() throws Exception {
        writeFile();
        byte[] data = new byte[4096];

        long start = System.currentTimeMillis();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(64)
                .readAhead(128).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            for (int i = 0; i < BLOCKS; i++) {
                if (bis.read(data) == -1) {
                    break;
                }
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("AIO read elapsed: " + (stop - start) + " bytes=" + workingFile.length());
    }

    @Test
    public void testStreamAvailable() throws Exception {
        writeFile();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(64)
                .readAhead(5).readOnly().inputStream(); BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            Assert.assertEquals(LEN * 4096, ais.available());
        }
    }

    @Test
    public void testStreamPosition() throws Exception {
        writeFile();
        byte[] data = new byte[4096];
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(64)
                .readAhead(5).readOnly().inputStream(); BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            Assert.assertEquals(0, ais.position());
            bis.read(data);
            Assert.assertEquals(8192, ais.position());
        }
    }

    @Test
    public void testStreamSkip() throws Exception {
        writeFile();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(64)
                .readAhead(5).readOnly().inputStream(); BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            Assert.assertEquals(0, ais.position());
            Assert.assertEquals(8193, bis.skip(8193));
            Assert.assertEquals(8193, ais.position());
        }
    }

    @Test
    public void testStreamBlock() throws Exception {
        writeFile();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(1)
                .readAhead(5).readOnly().inputStream(); BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            int expectedBlock = 0;
            LOG.debug("Expected blocks: " + ais.block(workingFile.length()));
            long wm = workingFile.length() - 512;
            for (long p = 0; p <= workingFile.length(); p++) {
                if (p > 0 && p % (1 * 4096) == 0) {
                    expectedBlock++;
                }
                Assert.assertEquals(expectedBlock, ais.block(p));
                // Speed up tests...
                if (p < wm) {
                    p += 511;
                }
            }
            Assert.assertEquals(LEN, expectedBlock);

        }
    }

    @Test
    public void testStreamSeek() throws Exception {
        writeFile();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(128).blocks(1)
                .readAhead(5).readOnly().inputStream()) {
            Assert.assertEquals(0, ais.seek(-1));
            Assert.assertEquals(0, ais.seek(workingFile.length() + 1));
            Assert.assertEquals(0, ais.position());
            Assert.assertEquals(512, ais.seek(512));
            Assert.assertEquals(7681, ais.seek(8193));
            Assert.assertEquals(8193, ais.position());
            Assert.assertEquals(-7681, ais.seek(512));
            Assert.assertEquals(512, ais.position());
            Assert.assertEquals(11788, ais.seek(4096 * 3 + 12));
            Assert.assertEquals(4096 * 3 + 12, ais.position());
        }
    }

    @Test
    public void testBufferStream() throws Exception {

        byte[] chars = "this is a special test".getBytes("UTF-8");
        byte[] readbuf = new byte[512];
        ByteBuffer.wrap(readbuf).put(chars);

        Assert.assertTrue(workingFile.exists());

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                .useFdataSync(true).useDirectIO(false).outputStream()) {
            aos.write(chars);
        }

        Assert.assertEquals(22, workingFile.length());

        try (

                AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                        .readAhead(1).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            int read = bis.read(chars);
            Assert.assertEquals(chars.length, read);
        }
    }

    @Test
    public void testDirectStream() throws Exception {

        byte[] chars = "this is a special test".getBytes("UTF-8");
        byte[] readbuf = new byte[512];
        ByteBuffer.wrap(readbuf).put(chars);

        Assert.assertTrue(workingFile.exists());

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                .useFdataSync(true).useDirectIO(true).outputStream()) {
            aos.write(chars);
        }

        Assert.assertEquals(22, workingFile.length());

        try (

                AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                        .readAhead(1).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            int read = bis.read(chars);
            Assert.assertEquals(chars.length, read);
        }
    }

    @Test
    public void testFlushDirectStream() throws Exception {

        byte[] chars = "this is a special test".getBytes("UTF-8");
        byte[] readbuf = new byte[512];
        ByteBuffer.wrap(readbuf).put(chars);

        Assert.assertTrue(workingFile.exists());

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                .useFdataSync(true).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos)) {
            for (int i = 0; i < 5; i++) {
                bos.write(chars);
                if (i != 4) {
                    bos.flush();
                }
            }
        }

        Assert.assertEquals(22 * 5, workingFile.length());

        int count;
        count = 0;
        try (

                AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                        .readAhead(1).readOnly().inputStream()) {
            while (ais.read() != -1) {
                ++count;
            }
        }
        Assert.assertEquals(22 * 5, count);

        count = 0;
        try (

                AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(2).blocks(2)
                        .readAhead(2).readOnly().inputStream()) {
            while (ais.read() != -1) {
                ++count;
            }
        }
        Assert.assertEquals(22 * 5, count);

        count = 0;
        try (

                AsyncInputStream ais = new AsyncStreamContext.Builder().file(workingFile).queueSize(2).blocks(2)
                        .readAhead(2).readOnly().useDirectIO(true).inputStream()) {
            while (ais.read() != -1) {
                ++count;
            }
        }
        Assert.assertEquals(22 * 5, count);

    }

    @Test
    public void testAppendDirect() throws Exception {

        byte[] chars = "this is a special test".getBytes("UTF-8");
        byte[] readbuf = new byte[512];
        ByteBuffer.wrap(readbuf).put(chars);

        Assert.assertTrue(workingFile.exists());

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(1).blocks(1)
                .useFdataSync(true).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos)) {
            for (int i = 0; i < 5; i++) {
                bos.write(chars);
            }
        }

        Assert.assertEquals(22 * 5, workingFile.length());

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).append().queueSize(1).blocks(1)
                .useFdataSync(true).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos)) {
            for (int i = 0; i < 5; i++) {
                bos.write(chars);
            }
        }

        Assert.assertEquals(22 * 10, workingFile.length());
    }

    @Test
    public void testMemoryBlocks() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        for (int i = 0; i < 4096; i++) {
            buffer.put((byte) 'a');
        }
        buffer.rewind();
        byte[] data = buffer.array();

        try (AsyncOutputStream aos = new AsyncStreamContext.Builder().file(workingFile).queueSize(5)
                .maxMemory(8 * 1024 * 1024)
                .useFdataSync(false).useDirectIO(true).outputStream();
                BufferedOutputStream bos = new BufferedOutputStream(aos, 8192)) {
            for (int i = 0; i < LEN; i++) {
                bos.write(data);
            }
        }

    }

}
