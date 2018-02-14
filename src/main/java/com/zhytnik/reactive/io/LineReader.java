package com.zhytnik.reactive.io;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;
import java.util.logging.Logger;

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
            int readLimit = chunk.limit();
            int nextStart = read(chunk, readLimit);

            if (request.isActive()) {
                chunk.limit(readLimit);
                lastChunk = chunk.position(nextStart).mark();
            } else {
                interrupter.run();
            }
        }

        private int read(ByteBuffer chunk, int limit) {
            int readStart = chunk.position();
            int lineStart = chunk.reset().position();
            byte[] memory = chunk.array();

            for (int i = readStart; i < limit; i++) {
                final byte c = memory[i];

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        ignoreLF = true;
                    } else if (ignoreLF) {
                        ignoreLF = false;
                        if (lineStart == i) {
                            lineStart = i + 1;
                            continue;
                        }
                    }

                    chunk.limit(i).position(lineStart);
                    request.send(chunk);

                    lineStart = i + 1;

                    if (!request.isActive()) break;
                }
            }
            return lineStart;
        }

        @Override
        public void onComplete() {
            if (lastChunk != null && lastChunk.reset().hasRemaining()) {
                request.send(lastChunk);
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
                remain = Math.addExact(remain, lines);
            } else {
                onError(new IllegalArgumentException("Requested line count should not be negative!"));
            }
        }

        private void send(ByteBuffer chunk) {
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
        private static final int GENERAL_MEMORY_SIZE = 8 * PAGE_SIZE;

        private ByteBuffer temporal;
        private final ByteBuffer general;

        MemoryAllocator() {
            general = ByteBuffer
                    .allocate(GENERAL_MEMORY_SIZE)
                    .limit(0)
                    .mark();
        }

        @Override
        public ByteBuffer get() {
            final ByteBuffer memory = fetchMemory();
            if (tryAddPage(memory) || tryCompact(memory)) {
                return memory;
            } else {
                return swapToTemporal(memory);
            }
        }

        private ByteBuffer fetchMemory() {
            return temporal == null ? general : trySwapToGeneral();
        }

        private boolean tryAddPage(ByteBuffer memory) {
            if (memory.capacity() - memory.limit() >= PAGE_SIZE) {
                addPage(memory, memory.limit());
                return true;
            }
            return false;
        }

        private boolean tryCompact(ByteBuffer memory) {
            if (memory.reset().position() >= PAGE_SIZE) {
                compress(memory);
                return true;
            }
            return false;
        }

        private void addPage(ByteBuffer memory, int to) {
            memory.position(to).limit(to + PAGE_SIZE);
        }

        private void compress(ByteBuffer memory) {
            final int payload = memory.limit() - memory.position();

            memory.compact();
            prepareForRead(memory);
            addPage(memory, payload);
        }

        private void prepareForRead(ByteBuffer memory) {
            memory.position(0).mark();
        }

        private ByteBuffer trySwapToGeneral() {
            if (temporal.limit() - temporal.reset().position() > (GENERAL_MEMORY_SIZE - PAGE_SIZE)) {
                return temporal;
            }
            general.position(0);
            general.put(temporal);
            prepareForRead(general);
            general.limit(temporal.limit() - temporal.reset().position());

            temporal = null;
            return general;
        }

        private ByteBuffer swapToTemporal(ByteBuffer memory) {
            Logger.getLogger("MemoryAllocator").warning("Using additional memory!");

            final int payload = memory.capacity() - memory.position();

            final ByteBuffer target = ByteBuffer
                    .allocate(2 * memory.capacity())
                    .put(memory);

            prepareForRead(target);
            addPage(target, payload);

            temporal = target;
            return target;
        }
    }
}
