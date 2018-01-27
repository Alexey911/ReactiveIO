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
        final MemoryAllocator allocator = new MemoryAllocator();

        final ParseRequest r = new ParseRequest(allocator, subscriber);
        subscriber.onSubscribe(r);

        final FileReader reader = new FileReader(path, allocator);
        final LineParser parser = new LineParser(r);
        parser.allocator = allocator;

        reader.subscribe(parser);
    }

    private static final class LineParser implements Subscriber<ByteBuffer> {

        private final ParseRequest request;

        private MemoryAllocator allocator;

        int lineFrom;
        boolean skip;

        LineParser(ParseRequest request) {
            this.request = request;
        }

        @Override
        public void onSubscribe(Subscription reader) {
            reader.request(Long.MAX_VALUE);
            request.setInterrupter(reader::cancel);
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            allocator.lock(buffer);

            for (int i = buffer.position(), max = buffer.limit(); !request.isDone() && i < max; i++) {
                final char c = (char) buffer.get(i);

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        skip = true;
                    } else if (skip) {
                        lineFrom = i + 1;
                        skip = false;
                        continue;
                    }

                    buffer.limit(i);
                    buffer.position(lineFrom);

                    request.accept(buffer);

                    lineFrom = i + 1;
                    buffer.limit(max);
                }
            }

            allocator.release(buffer);
        }

        @Override
        public void onComplete() {
            if (!request.interrupted && !request.isDone() && lineFrom < request.allocator.getLastReleased().limit()) {
                request.allocator.getLastReleased().position(lineFrom);
                request.accept(request.allocator.getLastReleased());
            }

            request.close();
        }

        @Override
        public void onError(Throwable e) {
            request.fail(e);
        }
    }

    private static final class ParseRequest implements Subscription, Closeable {

        private long lines;
        private boolean interrupted;

        private Runnable readInterrupter;

        private final MemoryAllocator allocator;
        private final Subscriber<? super ByteBuffer> subscriber;

        ParseRequest(MemoryAllocator allocator,
                     Subscriber<? super ByteBuffer> subscriber) {
            this.allocator = allocator;
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

        public void fail(Throwable error) {
            subscriber.onError(error);
        }

        private void accept(ByteBuffer memory) {
            subscriber.onNext(memory);
            decrease();
        }

        private void decrease() {
            this.lines--;

            if (lines == 0 && readInterrupter != null) {
                readInterrupter.run();
                readInterrupter = null;
            }
        }

        private void setInterrupter(Runnable on) {
            this.readInterrupter = on;
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
