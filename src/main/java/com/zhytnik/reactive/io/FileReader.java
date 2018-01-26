package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
class FileReader implements Publisher<ByteBuffer> {

    private final Path path;
    private final MemoryAllocator allocator;

    public FileReader(Path path, MemoryAllocator allocator) {
        this.path = path;
        this.allocator = allocator;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> reader) {
        try (final ReadRequest r = new ReadRequest(path, reader)) {
            reader.onSubscribe(r);

            while (!r.isDone()) {
                ByteBuffer memory = allocator.get();
                int progress = r.resource.read(memory, r.position());

                reader.onNext(memory);
                r.update(progress);
            }
        } catch (IOException error) {
            reader.onError(error);
        }
    }

    private static final class ReadRequest implements Subscription, Closeable {

        private long limit;
        private long position;
        private boolean interrupted;

        private final long max;
        private final FileChannel resource;
        private final Subscriber subscriber;

        public ReadRequest(Path path, Subscriber subscriber) throws IOException {
            this.resource = FileChannel.open(path);
            this.max = resource.size();
            this.subscriber = subscriber;
        }

        public boolean isDone() {
            return interrupted || position == limit;
        }

        public long position() {
            return position;
        }

        public void update(int progress) {
            position += Math.max(progress, 0);
        }

        @Override
        public void request(long byteCount) {
            if (byteCount == Long.MAX_VALUE || limit + byteCount <= max) {
                limit = Math.min(limit + byteCount, max);
            } else {
                interrupted = true;
                subscriber.onError(new IllegalArgumentException("The file contains only " + max));
            }
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        @Override
        public void close() throws IOException {
            resource.close();

            if (!interrupted && position == limit) {
                subscriber.onComplete();
            }
        }
    }
}
