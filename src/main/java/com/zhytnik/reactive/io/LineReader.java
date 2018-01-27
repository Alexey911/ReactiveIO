package com.zhytnik.reactive.io;

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
    public void subscribe(Subscriber<? super ByteBuffer> printer) {
        final ParseRequest subscription = new ParseRequest();
        printer.onSubscribe(subscription);

        final MemoryAllocator allocator = new MemoryAllocator();
        final LineParser parser = new LineParser(printer, subscription, allocator);
        final FileReader reader = new FileReader(path, allocator);

        reader.subscribe(parser);
    }

    static final class LineParser implements Subscriber<ByteBuffer> {

        private Runnable ioInterrupter;
        private final ParseRequest parse;
        private final Subscriber<? super ByteBuffer> reader;

        private final MemoryAllocator memory;

        public LineParser(Subscriber<? super ByteBuffer> reader,
                          ParseRequest parse,
                          MemoryAllocator memory) {
            this.reader = reader;
            this.memory = memory;
            this.parse = parse;
        }

        @Override
        public void onSubscribe(Subscription fileReader) {
            fileReader.request(Long.MAX_VALUE);
            ioInterrupter = fileReader::cancel;
        }

        int lineFrom = 0;
        boolean skip = false;

        @Override
        public void onNext(ByteBuffer buffer) {
            buffer.limit(buffer.position());
            buffer.reset();

            for (int i = buffer.position(), limit = buffer.limit(); i < limit && !parse.isDone(); i++) {
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

                    reader.onNext(buffer);
                    parse.decrease();
                    lineFrom = i + 1;

                    buffer.limit(limit);
                }
            }

            if (parse.isDone()) {
                ioInterrupter.run();
            }
        }

        @Override
        public void onComplete() {
            if (!parse.interrupted && !parse.isDone() && lineFrom < memory.getLastReleased().limit()) {
                memory.getLastReleased().position(lineFrom);
                reader.onNext(memory.getLastReleased());
                parse.decrease();
            }

            if (parse.isDone() && !parse.interrupted) {
                reader.onComplete();
            } else {
                reader.onError(new RuntimeException("There's no more line for reading!"));
            }
        }

        @Override
        public void onError(Throwable e) {
            reader.onError(e);
        }
    }

    static final class ParseRequest implements Subscription {

        private long lines;
        private boolean interrupted;

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

        private void decrease() {
            this.lines--;
        }
    }
}
