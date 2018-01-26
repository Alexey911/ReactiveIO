package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
class FileReader implements Processor<ByteBuffer, ByteBuffer> {

    private final Path path;

    private Allocator allocator;

    public FileReader(Path path) {
        this.path = path;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        if (allocator == null) {
            s.onError(new IllegalStateException());
            return;
        }

        final ReadRequest request = new ReadRequest();
        s.onSubscribe(request);

        try (InnerReader reader = new InnerReader(path, request, allocator::allocate)) {
            reader.read(s::onNext);
        } catch (IOException e) {
            request.fail();
            s.onError(e);
        }

        if (request.isSuccessful()) {
            s.onComplete();
        }
    }

    @Override
    public void onSubscribe(Subscription memoryProvider) {
        allocator = new Allocator(memoryProvider);
    }

    @Override
    public void onNext(ByteBuffer buffer) {
        allocator.release(buffer);
    }

    @Override
    public void onError(Throwable e) {
    }

    @Override
    public void onComplete() {
    }

    static final class InnerReader implements Closeable {

        private final FileChannel channel;
        private final ReadRequest request;
        private final Supplier<ByteBuffer> allocator;

        public InnerReader(Path path,
                           ReadRequest request,
                           Supplier<ByteBuffer> allocator) throws IOException {
            this.request = request;
            this.allocator = allocator;
            this.channel = FileChannel.open(path);
        }

        public void read(Consumer<ByteBuffer> reader) throws IOException {
            long pos = 0L, max = channel.size();

            while (pos < max && request.size > 0 && !request.canceled) {
                final ByteBuffer memory = allocator.get();
                int offset = Math.max(channel.read(memory, pos), 0);

                pos += offset;
                request.size -= offset;

                reader.accept(memory);
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private static final class ReadRequest implements Subscription {

        private long size;

        private boolean failed;
        private boolean canceled;
        private boolean unbounded;

        @Override
        public void request(long byteCount) {
            this.size += byteCount;
            this.unbounded = (Long.MAX_VALUE == byteCount); //TODO
        }

        @Override
        public void cancel() {
            this.canceled = true;
        }

        public void fail() {
            this.failed = true;
        }

        public boolean isSuccessful() {
            return !failed && !canceled;
        }
    }

    private static final class Allocator {

        private ByteBuffer freeMemory;
        private final Subscription provider;

        public Allocator(Subscription provider) {
            this.provider = provider;
        }

        public ByteBuffer allocate() {
            provider.request(1);
            return getExclusive();
        }

        private ByteBuffer getExclusive() {
            ByteBuffer memory = freeMemory;
            freeMemory = null;
            return memory;
        }

        public void release(ByteBuffer memory) {
            freeMemory = memory;
        }
    }
}
