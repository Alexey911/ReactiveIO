package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
class FileReader implements Publisher<ByteBuffer> {

    private final Path path;

    public FileReader(Path path) {
        this.path = path;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        boolean failed = false;

        try (InnerChannelReader r = new InnerChannelReader(path)) {
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

    static final class InnerChannelReader implements Subscription, Closeable {

        private static final int PAGE_SIZE = 4096;

        private final FileChannel channel;

        private boolean canceled;

        public InnerChannelReader(Path path) throws IOException {
            this.channel = FileChannel.open(path);
        }

        @Override
        public void request(long n) {
        }

        public void read(Consumer<ByteBuffer> consumer) throws Exception {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
            buffer.order(ByteOrder.nativeOrder());

            final long size = channel.size();
            long position = 0L;

            while (position < size && !canceled) {
                buffer.rewind();
                buffer.limit(PAGE_SIZE);

                position += Math.max(channel.read(buffer, position), 0);
                buffer.flip();

                consumer.accept(buffer.asReadOnlyBuffer());
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
