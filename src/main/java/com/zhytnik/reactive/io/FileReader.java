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
 * A file reader which reads requested bytes of files by {@link ByteBuffer}.
 * Needs custom memory provider {@link ReadSubscription#setAllocator(Supplier)}.
 *
 * @author Alexey Zhytnik
 */
public class FileReader implements Publisher<ByteBuffer> {

    private final Path path;

    /**
     * Constructs a FileReader associated with the file.
     *
     * @param path the path to file for reading
     */
    public FileReader(Path path) {
        this.path = path;
    }

    /**
     * Reads the file. Fails fast on any {@link IOException}.
     * Reads file content by ByteBuffers provided by custom memory allocator until
     * requested byte count is read. Invokes {@link Subscriber#onNext(Object)}
     * only with content which is placed from position (inclusive) to limit,
     * never invokes {@link Subscriber#onNext(Object)} without file content.
     * Warning: the file content should not be modified during subscription,
     * otherwise the result of the execution is undefined.
     *
     * @param subscriber the subscriber-reader
     * @see ReadSubscription
     */
    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        try (final ReadRequest r = new ReadRequest(path, subscriber)) {
            subscriber.onSubscribe(r);

            if (r.isActive()) r.tryInitialize();

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

    /**
     * Represents a file reading subscription.
     *
     * @author Alexey Zhytnik
     */
    public interface ReadSubscription extends Subscription {

        /**
         * Installs memory allocator which provides a memory for file reading.
         * Each invocation of memory allocator should return a ByteBuffer whose bytes
         * from position (inclusive) to limit will be used for writing file content,
         * but not all that bytes will be really used and limit position could be decreased.
         *
         * @param allocator the memory allocator
         */
        void setAllocator(Supplier<ByteBuffer> allocator);

        /**
         * Adds bytes for reading. Needs installed memory allocator,
         * otherwise throws {@link IllegalStateException}.
         * A value of {@code Long.MAX_VALUE} is request to read all file,
         * in other cases if requested byte count is negative or greater
         * than the file's size then {@link IllegalArgumentException} will be thrown.
         *
         * @param bytes the additional count of bytes for read
         */
        @Override
        void request(long bytes);

        /**
         * Stops reading, all used resources will be released after invoking.
         */
        @Override
        void cancel();
    }

    private static final class ReadRequest implements ReadSubscription, Closeable {

        private long max;
        private long limit;
        private long position;
        private boolean interrupted;
        private FileChannel resource;
        private Supplier<ByteBuffer> allocator;

        private final Path path;
        private final Subscriber subscriber;

        private ReadRequest(Path path, Subscriber subscriber) {
            this.path = path;
            this.max = Long.MAX_VALUE;
            this.subscriber = subscriber;
        }

        private void tryInitialize() throws IOException {
            resource = FileChannel.open(path, StandardOpenOption.READ);
            max = resource.size();

            long requestedBytes = limit;
            limit = 0;
            request(requestedBytes);
        }

        @Override
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
                onError(new IllegalStateException("Memory allocator isn't installed!"));
            } else if (bytes == Long.MAX_VALUE) {
                limit = max;
            } else if (bytes >= 0 && Math.addExact(limit, bytes) <= max) {
                limit += bytes;
            } else {
                onError(new IllegalArgumentException(path + " contains only " + max + " bytes!"));
            }
        }

        private void onError(Throwable error) {
            interrupted = true;
            subscriber.onError(error);
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        @Override
        public void close() {
            if (resource != null) {
                try {
                    resource.close();
                } catch (IOException e) {
                    if (!interrupted) onError(e);
                }
            }

            if (!interrupted && position == limit) {
                subscriber.onComplete();
            }
        }
    }
}
