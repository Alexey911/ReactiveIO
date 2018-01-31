package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;

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

            if (r.isActive()) reader.subscribe(parser);
        } catch (Exception e) {
            subscriber.onError(e);
        }
    }

    private static final class LineParser implements Subscriber<ByteBuffer> {

        private boolean ignoreLF;
        private Runnable interrupter;
        private ByteBuffer lastChunk;

        private final ParseRequest request;

        private LineParser(ParseRequest request) {
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

            for (int i = readStart, limit = chunk.limit(); i < limit && request.isActive(); i++) {
                final int c = chunk.get(i);

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        ignoreLF = true;
                    } else if (ignoreLF) {
                        lineStart = i + 1;
                        ignoreLF = false;
                        continue;
                    }

                    request.send(chunk, lineStart, i);

                    chunk.limit(limit);
                    lineStart = i + 1;
                }
            }

            if (request.isActive()) {
                lastChunk = chunk.position(lineStart).mark();
            } else {
                interrupter.run();
            }
        }

        @Override
        public void onComplete() {
            if (lastChunk != null && lastChunk.reset().position() <= lastChunk.limit()) {
                request.send(lastChunk, lastChunk.position(), lastChunk.limit());
            }
        }

        @Override
        public void onError(Throwable e) {
            request.onError(e);
        }
    }

    private static final class ParseRequest implements Subscription, Closeable {

        private long remain;
        private boolean unbounded;
        private boolean interrupted;

        private final Subscriber<? super ByteBuffer> subscriber;

        private ParseRequest(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        private boolean isActive() {
            return !interrupted && (unbounded || remain > 0);
        }

        @Override
        public void request(long lines) {
            if (lines == Long.MAX_VALUE) {
                unbounded = true;
            } else if (lines >= 0) {
                remain += lines;
            } else {
                onError(new IllegalArgumentException("Requested line count should not be negative!"));
            }
        }

        private void send(ByteBuffer chunk, int start, int end) {
            chunk.limit(end);
            chunk.position(start);
            subscriber.onNext(chunk);

            if (!unbounded) remain--;
        }

        private void onError(Throwable error) {
            interrupted = true;
            subscriber.onError(error);
        }

        @Override
        public void cancel() {
            interrupted = true;
        }

        @Override
        public void close() {
            if (interrupted) return;

            if (unbounded || remain == 0) {
                subscriber.onComplete();
            } else {
                subscriber.onError(new RuntimeException("There's no more line for reading!"));
            }
        }
    }

    static final class MemoryAllocator implements Supplier<ByteBuffer> {

        private static final int PAGE_SIZE = 4096;
        private static final int DIRECT_MEMORY_SIZE = 8 * PAGE_SIZE;

        private ByteBuffer heap;
        private final ByteBuffer direct;

        MemoryAllocator() {
            direct = ByteBuffer
                    .allocateDirect(DIRECT_MEMORY_SIZE)
                    .order(ByteOrder.nativeOrder())
                    .limit(0)
                    .mark();
        }

        @Override
        public ByteBuffer get() {
            final ByteBuffer memory = isDirectMode() ? direct : trySwapToDirect();

            if (memory.capacity() - memory.limit() >= PAGE_SIZE) {
                return addPage(memory, memory.limit());
            } else if (hasGarbage(memory)) {
                return compress(memory);
            } else {
                return swapToHeap(memory);
            }
        }

        private boolean isDirectMode() {
            return heap == null;
        }

        private ByteBuffer addPage(ByteBuffer memory, int to) {
            return memory.position(to).limit(to + PAGE_SIZE);
        }

        private boolean hasGarbage(ByteBuffer memory) {
            return memory.reset().position() >= PAGE_SIZE;
        }

        private ByteBuffer compress(ByteBuffer memory) {
            final int start = memory.position();

            memory.compact();
            prepareForRead(memory);
            addPage(memory, DIRECT_MEMORY_SIZE - start);
            return memory;
        }

        private void prepareForRead(ByteBuffer memory) {
            memory.position(0).mark();
        }

        private ByteBuffer trySwapToDirect() {
            if (heap.limit() - heap.reset().position() > (DIRECT_MEMORY_SIZE - PAGE_SIZE)) return heap;

            direct.position(0);
            direct.put(heap);
            prepareForRead(direct);
            direct.limit(heap.limit() - heap.reset().position());

            heap = null;
            return direct;
        }

        private ByteBuffer swapToHeap(ByteBuffer memory) {
            final int payload = memory.capacity() - memory.position();

            final ByteBuffer target = ByteBuffer
                    .allocate(2 * memory.capacity())
                    .put(memory);

            prepareForRead(target);
            target.limit(payload + PAGE_SIZE);
            target.position(payload);

            heap = target;
            return target;
        }
    }
}
