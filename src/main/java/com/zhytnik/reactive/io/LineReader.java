package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
public class LineReader implements Publisher<ByteBuffer> {

    private final Path path;

    public LineReader(Path path) {
        this.path = path;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        final InnerLineReader r = new InnerLineReader(s);

        s.onSubscribe(r);
        final FileReader ioReader = new FileReader(path);

        r.subscribe(ioReader);
        ioReader.subscribe(r);
    }

    static final class InnerLineReader implements Processor<ByteBuffer, ByteBuffer>, Subscription {

        private static final int PAGE_SIZE = 4096;

        private long lineCount;
        private boolean unboundReading;
        private Runnable ioInterrupter;
        private final Subscriber<? super ByteBuffer> reader;

        public InnerLineReader(Subscriber<? super ByteBuffer> reader) {
            this.reader = reader;
        }

        @Override
        public void request(long lineCount) {
            this.lineCount = lineCount;
            this.unboundReading = (lineCount == Long.MAX_VALUE);
        }

        @Override
        public void cancel() {
            lineCount = 0;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
            ioInterrupter = s::cancel;
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            //TODO: use slice
            int from = 0, limit = buffer.limit();

            for (int i = 0; i < limit && lineCount > 0; i++) {
                final int c = buffer.get(i);

                if (c == '\r' || c == '\n') {
                    buffer.position(from);
                    buffer.limit(i);

                    reader.onNext(buffer);
                    lineCount--;
                    buffer.limit(limit);

                    if (c == '\r' && i + 1 < limit && buffer.get(i + 1) == '\n') {
                        i++;
                    }
                    from = i + 1;
                }
            }

            if (from < limit && lineCount-- >= 0) {
                buffer.position(from);
                reader.onNext(buffer);
            }

            if (lineCount <= 0) {
                ioInterrupter.run();
            }
        }

        @Override
        public void onComplete() {
            if (unboundReading || lineCount <= 0) {
                reader.onComplete();
            } else {
                reader.onError(new RuntimeException("There's no more line for reading!"));
            }
        }

        @Override
        public void onError(Throwable e) {
            reader.onError(e);
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });

            final ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
            buffer.order(ByteOrder.nativeOrder());

            buffer.rewind();
            buffer.limit(PAGE_SIZE);

            subscriber.onNext(buffer);
        }
    }
}
