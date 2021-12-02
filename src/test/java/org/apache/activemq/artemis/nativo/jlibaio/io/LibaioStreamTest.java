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

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test is using a different package from {@link LibaioFile} as I need to
 * validate public methods on the API
 */
public class LibaioStreamTest {
    @Rule
    public TemporaryFolder temporaryFolder;

    @After
    public void deleteFactory() {
        validateLibaio();
    }

    public void validateLibaio() {
        Assert.assertEquals(0, LibaioContext.getTotalMaxIO());
    }

    public LibaioStreamTest() {
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
    public void testSystemStream() throws Exception {

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        for (int i = 0; i < 4096; i++) {
            buffer.put((byte) 'a');
        }
        buffer.rewind();
        byte[] data = buffer.array();

        final long blocks = 4096 * BLOCKS;

        long start = System.currentTimeMillis();
        File outFile = temporaryFolder.newFile("test.bin"); // new File("target/test.out" +
                                                            // System.currentTimeMillis()).getCanonicalFile();
        try (FileOutputStream fos = new FileOutputStream(outFile);
                BufferedOutputStream bos = new BufferedOutputStream(fos);) {
            for (int i = 0; i < blocks; i++) {
                fos.write(data);
            }
        }
        long stop = System.currentTimeMillis();

        System.out.println("System write elapsed: " + (stop - start));

        Assert.assertEquals(blocks * 4096, outFile.length());

        start = System.currentTimeMillis();
        try (FileInputStream fis = new FileInputStream(outFile);
                BufferedInputStream bis = new BufferedInputStream(fis, 8192)) {
            for (int i = 0; i < blocks; i++) {
                if (bis.read(data) == -1) {
                    break;
                }
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("System read elapsed: " + (stop - start) + " bytes=" + outFile.length());
    }

    public static final long BLOCKS = 512;

    @Test
    public void testAsyncStream() throws Exception {

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        for (int i = 0; i < 4096; i++) {
            buffer.put((byte) 'a');
        }
        buffer.rewind();
        byte[] data = buffer.array();

        final long blocks = 4096 * BLOCKS;

        File outFile = temporaryFolder.newFile("test.bin"); // new File("target/test.bin" +
                                                            // System.currentTimeMillis()).getCanonicalFile();
        // AsyncOutputStream tmp = new
        // AsyncStreamContext.Builder().file(outFile).queueSize(50).bufferSize(256 *
        // 4096)
        // .useFdataSync(false).useDirectIO(true).outputStream();
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

        Assert.assertEquals(blocks * 4096, outFile.length());

        start = System.currentTimeMillis();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(outFile).queueSize(128).blocks(64)
                .readAhead(128).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            for (int i = 0; i < blocks; i++) {
                if (bis.read(data) == -1) {
                    break;
                }
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("AIO read elapsed: " + (stop - start) + " bytes=" + outFile.length());

        start = System.currentTimeMillis();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(outFile).queueSize(128).blocks(64)
                .readAhead(128).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            for (int i = 0; i < blocks; i++) {
                if (bis.read(data) == -1) {
                    break;
                }
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("AIO cached read elapsed: " + (stop - start) + " bytes=" + outFile.length());

        start = System.currentTimeMillis();
        try (AsyncInputStream ais = new AsyncStreamContext.Builder().file(outFile).queueSize(128).blocks(64)
                .readAhead(128).readOnly().inputStream();
                BufferedInputStream bis = new BufferedInputStream(ais, 8192)) {
            for (int i = 0; i < blocks; i++) {
                if (i == 10) {
                    break;
                }
                if (bis.read(data) == -1) {
                    break;
                }
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("AIO interrupted read elapsed: " + (stop - start) + " bytes=" + outFile.length());
    }

}
