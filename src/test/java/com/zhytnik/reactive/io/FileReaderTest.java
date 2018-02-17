package com.zhytnik.reactive.io;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

import static java.lang.Long.MAX_VALUE;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.write;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Alexey Zhytnik
 * @since 31.01.2018
 */
public class FileReaderTest {

    @ClassRule
    public static TemporaryFolder files = new TemporaryFolder();

    File file;
    FileReader reader;
    ReadAssertionSubscriber subscriber;
    List<byte[]> preparedChunks;

    @Before
    public void setUp() throws Exception {
        file = files.newFile();
        reader = new FileReader();
        subscriber = new ReadAssertionSubscriber(file.toPath());
        preparedChunks = new ArrayList<>();
    }

    @Test
    public void failsOnWrongResources() {
        file.delete();
        subscriber.request = 2 * 4096;
        reader.subscribe(subscriber.asExpected(NoSuchFileException.class));
    }

    @Test
    public void failsOnMissedAllocator() {
        subscriber.allocator = null;
        reader.subscribe(subscriber.asExpected(IllegalStateException.class));
    }

    @Test
    public void failsOnWrongRequests() {
        subscriber.request = file.length() + 10;
        reader.subscribe(subscriber.asExpected(IllegalArgumentException.class));
    }

    @Test
    public void lazilyUsesResources() {
        subscriber.allocator = () -> null;
        reader.subscribe(subscriber);
    }

    @Test
    public void readsRequestedBytes() throws Exception {
        addDataForRead(chunk4KB(), chunk4KB(), chunk4KB());
        subscriber.request = 2 * 4096;
        reader.subscribe(subscriber);

        assertThat(subscriber.items).hasSize(2);
        assertThat(preparedChunks.get(0)).isEqualTo(subscriber.items.get(0));
        assertThat(preparedChunks.get(1)).isEqualTo(subscriber.items.get(1));
    }

    @Test
    public void doesNotTouchBytesBeforeReadPosition() throws Exception {
        addDataForRead(chunk4KB(), chunk4KB());

        subscriber
                .allocator.get()
                .position(777).mark()
                .put((byte) 7)
                .put((byte) 9);

        reader.subscribe(subscriber);

        assertThat(subscriber.allocator.get().reset().position()).isEqualTo(777);
        assertThat(subscriber.allocator.get().get(777)).isEqualTo((byte) 7);
        assertThat(subscriber.allocator.get().get(778)).isEqualTo((byte) 9);
    }

    @Test
    public void requestWithMaxLongIsWishToReadResourceFully() throws Exception {
        subscriber.request = MAX_VALUE;
        addDataForRead(chunk4KB(), chunk4KB(), chunk4KB());
        reader.subscribe(subscriber);

        assertThat(subscriber.items).hasSize(3);
        assertThat(preparedChunks.get(0)).isEqualTo(subscriber.items.get(0));
        assertThat(preparedChunks.get(1)).isEqualTo(subscriber.items.get(1));
        assertThat(preparedChunks.get(2)).isEqualTo(subscriber.items.get(2));
    }

    @Test
    public void readsEmptyResources() {
        subscriber.request = MAX_VALUE;
        reader.subscribe(subscriber);
    }

    @After
    public void tearDown() {
        if (file.exists() && !file.delete()) {
            fail("Maybe file is blocked by Reader!");
        }
        subscriber.validate();
    }

    byte[] chunk4KB() {
        final byte[] bytes = new byte[4096];
        new Random().nextBytes(bytes);

        preparedChunks.add(bytes);
        return bytes;
    }

    void addDataForRead(byte[]... chunks) throws Exception {
        for (byte[] chunk : chunks) {
            write(file.toPath(), chunk, APPEND);
        }
    }

    static class ReadAssertionSubscriber extends BaseAssertionSubscriber<ByteBuffer, byte[]> {

        final Path path;
        Supplier<ByteBuffer> allocator = new LineReader.MemoryAllocator();

        private ReadAssertionSubscriber(Path path) {
            this.path = path;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            super.onSubscribe(s);

            ((FileReader.ReadSubscription) s).setPath(path);
            ((FileReader.ReadSubscription) s).setAllocator(allocator);
            doRequest();
        }

        @Override
        public void onNext(ByteBuffer b) {
            super.onNext(b);

            assertThat(b).isNotNull();
            items.add(allocate(b.limit() - b.position()).put(b).array());
        }
    }
}