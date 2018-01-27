package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

        final Memory memory = new Memory();
        final MemoryAllocator allocator = new MemoryAllocator();
        memory.subscribe(allocator);

        final LineParser parser = new LineParser(printer, subscription, memory);

        final FileReader reader = new FileReader(path, allocator);
        reader.subscribe(parser);
    }

    static final class LineParser implements Subscriber<ByteBuffer> {

        private Runnable ioInterrupter;
        private final ParseRequest parse;
        private final Subscriber<? super ByteBuffer> reader;

        private final Memory memory;

        public LineParser(Subscriber<? super ByteBuffer> reader,
                          ParseRequest parse,
                          Memory memory) {
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

            for (int i = memory.readFrom, limit = buffer.limit(); i < limit && !parse.isDone(); i++) {
                final char c = (char) buffer.get(i);

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        skip = true;
                    } else if (skip) {
                        lineFrom = i + 1;
                        skip = false;
                        continue;
                    }

                    buffer.position(lineFrom);
                    buffer.limit(i);

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
            if (!skip && lineFrom < memory.memory.limit() && !parse.isDone()) {
                memory.memory.limit(memory.memory.position());
                memory.memory.position(lineFrom);
                reader.onNext(memory.memory);
                parse.decrease();
            }

            if (parse.isDone()) {
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

    static final class Memory implements Publisher<ByteBuffer>, Subscription {

        private static final int PAGE_SIZE = 3;

        private Subscriber<? super ByteBuffer> allocator;

        private ByteBuffer memory;

        int readFrom = 0;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> allocator) {
            this.allocator = allocator;
            this.allocator.onSubscribe(this);
        }

        public ByteBuffer allocate() {
            final ByteBuffer memory = ByteBuffer.allocateDirect(10 * PAGE_SIZE);
            memory.order(ByteOrder.nativeOrder());
            memory.limit(PAGE_SIZE);

            return this.memory = memory;
        }

        private int extend(ByteBuffer memory) {
            int prev = memory.limit();

            memory.limit(prev * 2);
            memory.position(prev);

            return prev;
        }

        @Override
        public void request(long memoryRequest) {
            try {
                if (memory == null) {
                    allocate();
                } else {
                    readFrom = extend(memory);
                }
                allocator.onNext(memory);
            } catch (Exception memoryError) {
                allocator.onError(memoryError);
            }
        }

        @Override
        public void cancel() {

        }
    }

    static final class ParseRequest implements Subscription {

        private long lines;

        @Override
        public void request(long lines) {
            this.lines += lines;
        }

        @Override
        public void cancel() {
        }

        private boolean isDone() {
            return lines <= 0;
        }

        private void decrease() {
            this.lines--;
        }
    }
}
