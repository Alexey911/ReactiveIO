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
        final LineReadingSubscription subscription = new LineReadingSubscription();
        s.onSubscribe(subscription);

        final LineParser parser = new LineParser(s, subscription);
        final FileReader reader = new FileReader(path);

        parser.subscribe(reader);
        reader.subscribe(parser);
    }

    static final class LineParser implements Processor<ByteBuffer, ByteBuffer> {

        private static final int PAGE_SIZE = 8;

        private Runnable ioInterrupter;
        private final LineReadingSubscription subscription;
        private final Subscriber<? super ByteBuffer> reader;

        public LineParser(Subscriber<? super ByteBuffer> reader,
                          LineReadingSubscription subscription) {
            this.reader = reader;
            this.subscription = subscription;
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

            for (int i = 0; i < limit && subscription.lineCount > 0; i++) {
                final int c = buffer.get(i);

                if (c == '\r' || c == '\n') {
                    buffer.position(from);
                    buffer.limit(i);

                    reader.onNext(buffer);
                    subscription.lineCount--;
                    buffer.limit(limit);

                    if (c == '\r' && i + 1 < limit && buffer.get(i + 1) == '\n') {
                        i++;
                    }
                    from = i + 1;
                }
            }

            if (from < limit && subscription.lineCount-- >= 0) {
                buffer.position(from);
                reader.onNext(buffer);
            }

            if (subscription.lineCount <= 0) {
                ioInterrupter.run();
            }
        }

        @Override
        public void onComplete() {
            if (subscription.unbounded || subscription.lineCount <= 0) {
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
                    final ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
                    buffer.order(ByteOrder.nativeOrder());

                    buffer.rewind();
                    buffer.limit(PAGE_SIZE);

                    subscriber.onNext(buffer);
                }

                @Override
                public void cancel() {
                }
            });
        }
    }

    static final class LineReadingSubscription implements Subscription {

        private long lineCount;
        private boolean unbounded;

        @Override
        public void request(long lineCount) {
            this.lineCount += lineCount;
            this.unbounded = (lineCount == Long.MAX_VALUE);
        }

        @Override
        public void cancel() {
            this.lineCount = 0;
        }
    }
}
