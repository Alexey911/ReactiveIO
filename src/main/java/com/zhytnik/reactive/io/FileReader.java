package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
public class FileReader implements Publisher<ByteBuffer> {

    private final Path path;

    public FileReader(Path path) {
        this.path = path;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        try (final ReadRequest r = new ReadRequest(path, subscriber)) {
            subscriber.onSubscribe(r);

            while (r.isActive()) {
                final ByteBuffer chunk = r.allocator.get();
                final int progress = r.resource.read(chunk, r.position());

                chunk.limit(chunk.position());
                chunk.position(chunk.limit() - progress);

                subscriber.onNext(chunk);
                r.update(progress);
            }
        } catch (Exception error) {
            subscriber.onError(error);
        }
    }

    public static final class ReadRequest implements Subscription, Closeable {

        private long limit;
        private long position;
        private boolean interrupted;

        private final long max;
        private final FileChannel resource;
        private final Subscriber subscriber;

        private Supplier<ByteBuffer> allocator;

        private ReadRequest(Path path, Subscriber subscriber) throws IOException {
            this.resource = FileChannel.open(path, StandardOpenOption.READ);
            this.max = resource.size();
            this.subscriber = subscriber;
        }

        public void setAllocator(Supplier<ByteBuffer> allocator) {
            this.allocator = allocator;
        }

        private boolean isActive() {
            return !interrupted && position < limit;
        }

        private long position() {
            return position;
        }

        private void update(int progress) {
            position += progress;
        }

        @Override
        public void request(long bytes) {
            if (allocator == null) {
                interrupted = true;
                subscriber.onError(new IllegalStateException("Memory allocator isn't installed!"));
            } else if (bytes == Long.MAX_VALUE || limit + bytes <= max) {
                limit = Math.min(limit + bytes, max);
            } else {
                interrupted = true;
                subscriber.onError(new IllegalArgumentException("The resource contains only " + max + " bytes!"));
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
