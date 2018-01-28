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
    public void subscribe(Subscriber<? super ByteBuffer> reader) {
        try (final ParseRequest r = new ParseRequest(reader)) {
            reader.onSubscribe(r);

            FileReader file = new FileReader(path, new MemoryAllocator());
            LineParser parser = new LineParser(r);

            file.subscribe(parser);
        } catch (Exception e) {
            reader.onError(e);
        }
    }

    private static final class LineParser implements Subscriber<ByteBuffer> {

        private int lineStart;
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
            buffer.reset();

            for (int i = buffer.position(), max = buffer.limit(); !request.isDone() && i < max; i++) {
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

                    request.accept(buffer);

                    lineStart = i + 1;
                    buffer.limit(max);
                }
            }

            if (request.isDone()) {
                breaker.run();
            } else {
                buffer.position(buffer.limit());
                buffer.mark();
                lastBuffer = buffer;
            }
        }

        @Override
        public void onComplete() {
            if (!request.isDone() && lineStart < lastBuffer.limit()) {
                lastBuffer.position(lineStart);
                request.accept(lastBuffer);
            }
        }

        @Override
        public void onError(Throwable e) {
            request.onError(e);
        }
    }

    private static final class ParseRequest implements Subscription, Closeable {

        private long lines;
        private boolean interrupted;

        private final Subscriber<? super ByteBuffer> subscriber;

        ParseRequest(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long lines) {
            this.lines += lines;
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        private boolean isDone() {
            return interrupted || lines == 0;
        }

        public void onError(Throwable error) {
            subscriber.onError(error);
        }

        private void accept(ByteBuffer memory) {
            subscriber.onNext(memory);
            lines--;
        }

        @Override
        public void close() {
            if (!interrupted && lines == 0) {
                subscriber.onComplete();
            } else {
                subscriber.onError(new RuntimeException("There's no more line for reading!"));
            }
        }
    }
}
