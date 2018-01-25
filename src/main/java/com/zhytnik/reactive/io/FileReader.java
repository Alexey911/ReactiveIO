package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
class FileReader implements Processor<ByteBuffer, ByteBuffer> {

    private final Path path;
    private ByteBuffer buffer;

    public FileReader(Path path) {
        this.path = path;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        boolean failed = false;

        try (InnerChannelReader r = new InnerChannelReader(path, this::getBuffer)) {
            s.onSubscribe(r);
            r.read(s::onNext);
        } catch (Exception e) {
            s.onError(e);
            failed = true;
        }

        if (!failed) {
            s.onComplete();
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(1);
    }

    @Override
    public void onNext(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void onError(Throwable e) {
    }

    @Override
    public void onComplete() {
    }

    private ByteBuffer getBuffer() {
        return buffer;
    }

    static final class InnerChannelReader implements Subscription, Closeable {

        private final FileChannel channel;

        private boolean canceled;

        private final Callable<ByteBuffer> bufferFactory;

        public InnerChannelReader(Path path, Callable<ByteBuffer> bufferFactory) throws IOException {
            this.channel = FileChannel.open(path);
            this.bufferFactory = bufferFactory;
        }

        @Override
        public void request(long n) {
        }

        public void read(Consumer<ByteBuffer> consumer) throws Exception {

            final long size = channel.size();
            long position = 0L;

            while (position < size && !canceled) {
                final ByteBuffer buffer = bufferFactory.call();
                position += Math.max(channel.read(buffer, position), 0);
                consumer.accept(buffer);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
