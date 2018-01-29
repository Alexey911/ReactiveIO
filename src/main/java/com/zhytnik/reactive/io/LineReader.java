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

            final FileReader reader = new FileReader(path);
            final LineParser parser = new LineParser(r);

            reader.subscribe(parser);
        } catch (Exception e) {
            subscriber.onError(e);
        }
    }

    private static final class LineParser implements Subscriber<ByteBuffer> {

        private boolean ignoreLF;
        private Runnable interrupter;
        private ByteBuffer lastChunk;

        private final ParseRequest request;

        LineParser(ParseRequest request) {
            this.request = request;
        }

        @Override
        public void onSubscribe(Subscription s) {
            ((FileReader.ReadRequest) s).setAllocator(new MemoryAllocator());
            s.request(Long.MAX_VALUE);
            interrupter = s::cancel;
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            int readStart = chunk.position();
            int lineStart = chunk.reset().position();

            for (int i = readStart, max = chunk.limit(); i < max && request.isActive(); i++) {
                final int c = chunk.get(i);

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        ignoreLF = true;
                    } else if (ignoreLF) {
                        lineStart = i + 1;
                        ignoreLF = false;
                        continue;
                    }

                    request.send(chunk, i);
                    lineStart = i + 1;
                }
            }

            if (!request.isActive()) {
                interrupter.run();
            } else {
                chunk.position(lineStart).mark();
                lastChunk = chunk;
            }
        }

        @Override
        public void onComplete() {
            if (lastChunk != null && lastChunk.reset().hasRemaining()) {
                request.send(lastChunk, lastChunk.limit());
            }
        }

        @Override
        public void onError(Throwable e) {
            request.onError(e);
        }
    }

    public static final class ParseRequest implements Subscription, Closeable {

        private long remain;
        private boolean unbounded;
        private boolean interrupted;

        private final Subscriber<? super ByteBuffer> subscriber;

        ParseRequest(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        private boolean isActive() {
            return !interrupted && (unbounded || remain > 0);
        }

        @Override
        public void request(long lines) {
            if (lines != Long.MAX_VALUE) {
                remain += lines;
            } else {
                unbounded = true;
            }
        }

        private void send(ByteBuffer chunk, int end) {
            final int limit = chunk.limit();

            subscriber.onNext(chunk.limit(end).asReadOnlyBuffer());

            chunk.limit(limit);

            if (!unbounded) remain--;
        }

        private void onError(Throwable error) {
            subscriber.onError(error);
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        @Override
        public void close() {
            if (!interrupted && (unbounded || remain == 0)) {
                subscriber.onComplete();
            } else {
                subscriber.onError(new RuntimeException("There's no more line for reading!"));
            }
        }
    }
}
