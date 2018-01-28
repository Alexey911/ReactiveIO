package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        try (final ParseRequest r = new ParseRequest(subscriber)) {
            subscriber.onSubscribe(r);

            final FileReader reader = new FileReader(path, new MemoryAllocator());
            final LineParser parser = new LineParser(r);

            reader.subscribe(parser);
        } catch (Exception e) {
            subscriber.onError(e);
        }
    }

    private static final class LineParser implements Subscriber<ByteBuffer> {

        private boolean ignoreLF;
        private Runnable breaker;
        private ByteBuffer lastBuffer;

        private final ParseRequest request;

        LineParser(ParseRequest request) {
            this.request = request;
        }

        @Override
        public void onSubscribe(Subscription read) {
            breaker = read::cancel;
            read.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            int start = buffer.position();
            int lineStart = buffer.reset().position();

            for (int i = start, max = buffer.limit(); i < max && request.isActive(); i++) {
                final int c = buffer.get(i);

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        ignoreLF = true;
                    } else if (ignoreLF) {
                        lineStart = i + 1;
                        ignoreLF = false;
                        continue;
                    }

                    buffer.limit(i);
                    buffer.position(lineStart);

                    request.send(buffer);

                    lineStart = i + 1;
                    buffer.limit(max);
                }
            }

            if (!request.isActive()) {
                breaker.run();
            } else {
                buffer.position(lineStart).mark();
                buffer.position(buffer.limit());

                lastBuffer = buffer;
            }
        }

        @Override
        public void onComplete() {
            int lineStart = lastBuffer.reset().position();

            if (lineStart < lastBuffer.limit()) {
                lastBuffer.position(lineStart);
                request.send(lastBuffer);
            }
        }

        @Override
        public void onError(Throwable e) {
            request.onError(e);
        }
    }

    private static final class ParseRequest implements Subscription, Closeable {

        private long lines;
        private boolean unbounded;
        private boolean interrupted;

        private final Subscriber<? super ByteBuffer> subscriber;

        ParseRequest(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long lines) {
            if (lines != Long.MAX_VALUE) {
                this.lines += lines;
            } else {
                this.unbounded = true;
            }
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        private boolean isActive() {
            return !interrupted && (unbounded || lines > 0);
        }

        private void send(ByteBuffer line) {
            subscriber.onNext(line);
            if (!unbounded) lines--;
        }

        private void onError(Throwable error) {
            subscriber.onError(error);
        }

        @Override
        public void close() {
            if (!interrupted && (unbounded || lines == 0)) {
                subscriber.onComplete();
            } else {
                subscriber.onError(new RuntimeException("There's no more line for reading!"));
            }
        }
    }
}
