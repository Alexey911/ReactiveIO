package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * A line by line file reader which reads requested lines by {@link ByteBuffer}.
 * Lazily reads lines and almost always uses only 32KB of memory.
 * Uses additional memory only for lines that are greater than 32768 characters (logs these allocations).
 * Detects lines that terminated by any one of a line feed ('\n'), a carriage return ('\r'),
 * a carriage return followed immediately by a line feed, or by reaching the end-of-file.
 * Supported charsets:
 * {@link StandardCharsets#UTF_8 UTF-8},
 * {@link StandardCharsets#US_ASCII US-ASCII},
 * {@link StandardCharsets#ISO_8859_1 ISO-8859-1}.
 *
 * @author Alexey Zhytnik
 */
public final class LineReader implements Publisher<ByteBuffer> {

    private final Path path;

    /**
     * Constructs a LineReader associated with the file.
     *
     * @param path the path to file for reading
     */
    public LineReader(Path path) {
        this.path = path;
    }

    /**
     * Reads the file by lines. Before invocation of {@link Subscription#request(long)}
     * doesn't consume any resources. Reads only requested count of lines,
     * a value of {@code Long.MAX_VALUE} is request to read all lines.
     * If at the end of the file requested line count isn't reached then
     * {@link NoSuchLineCountException} will be thrown, also throws
     * {@link IllegalArgumentException} on negative values of requests.
     * Invokes {@link Subscriber#onNext(Object)} with a line which is placed from
     * position to limit (exclusive), in case of empty files this method is never invoked.
     * Warnings: bytes of each line exist only inside invoked body of
     * {@link Subscriber#onNext(Object)}, do not change bytes after limit position (inclusive).
     *
     * @param subscriber the subscriber-reader
     * @see FileReader
     */
    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        try (final ParseRequest r = new ParseRequest(path, subscriber)) {
            subscriber.onSubscribe(r);

            if (r.isActive()) {
                final FileReader reader = new FileReader();
                final LineParser parser = new LineParser(r);
                reader.subscribe(parser);
            }
        }
    }

    /**
     * Thrown to indicate that end-of-file was reached without all requested lines.
     *
     * @author Alexey Zhytnik
     */
    public static final class NoSuchLineCountException extends RuntimeException {
        private NoSuchLineCountException(long remain) {
            super("There's no more line for reading, remaining line count is " + remain + "!");
        }
    }

    /**
     * Represents a FileReader subscriber which parses lines and
     * sends them to a LineReader's subscriber.
     *
     * @author Alexey Zhytnik
     */
    private static final class LineParser implements Subscriber<ByteBuffer> {

        private boolean ignoreLF;
        private Runnable interrupter;
        private ByteBuffer lastChunk;

        private final ParseRequest request;

        private LineParser(ParseRequest request) {
            this.request = request;
        }

        /**
         * Requests reading of the whole file.
         */
        @Override
        public void onSubscribe(Subscription s) {
            ((FileReader.ReadSubscription) s).setPath(request.path);
            ((FileReader.ReadSubscription) s).setAllocator(new MemoryAllocator());
            s.request(Long.MAX_VALUE);
            interrupter = s::cancel;
        }

        /**
         * Parses a file content into lines and sends
         * them to the {@link ParseRequest#subscriber}.
         * Between invocations saves start of last line at mark position.
         * Subscription cancellation stops file reading and
         * produces releasing used resources.
         *
         * @param chunk a file content from {@link FileReader}
         */
        @Override
        public void onNext(ByteBuffer chunk) {
            int readLimit = chunk.limit();
            int nextStart = parse(chunk, readLimit);

            if (request.isActive()) {
                chunk.limit(readLimit);
                lastChunk = chunk.position(nextStart).mark();
            } else {
                interrupter.run();
            }
        }

        private int parse(ByteBuffer chunk, int limit) {
            int readStart = chunk.position();
            int lineStart = chunk.reset().position();
            byte[] memory = chunk.array();
            boolean skipLF = ignoreLF;

            for (int i = readStart; i < limit; i++) {
                final byte c = memory[i];

                if (c == '\r' || c == '\n') {

                    if (c == '\r') {
                        skipLF = true;
                    } else if (skipLF) {
                        skipLF = false;
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
            ignoreLF = skipLF;
            return lineStart;
        }

        /**
         * Invoked when end of the file is reached.
         * If previously loaded bytes weren't fully sent,
         * sends them to the {@link ParseRequest#subscriber}
         */
        @Override
        public void onComplete() {
            if (lastChunk != null && lastChunk.reset().hasRemaining()) {
                request.send(lastChunk);
            }
        }

        /**
         * Redirects FileReader's exceptions and errors
         * to the {@link ParseRequest#subscriber}.
         */
        @Override
        public void onError(Throwable e) {
            request.onError(e);
        }
    }

    private static final class ParseRequest implements Subscription, AutoCloseable {

        private long remain;
        private boolean unbounded;
        private boolean interrupted;

        private final Path path;
        private final Subscriber<? super ByteBuffer> subscriber;

        private ParseRequest(Path path, Subscriber<? super ByteBuffer> subscriber) {
            this.path = path;
            this.subscriber = subscriber;
        }

        private boolean isActive() {
            return !interrupted && (unbounded || remain > 0);
        }

        @Override
        public void request(long lines) {
            if (lines == Long.MAX_VALUE) {
                remain = 0;
                unbounded = true;
            } else if (lines >= 0) {
                remain += lines;
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
                subscriber.onError(new NoSuchLineCountException(remain));
            }
        }
    }

    /**
     * Allocates memory by 4096-byte regions for file reading,
     * keeps bytes reserved by LineParser. When general memory capacity isn't enough
     * it tries to do compression and reusing, otherwise it will use
     * as much memory as needed with attempts to use general memory again.
     *
     * @author Alexey Zhytnik
     */
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

        /**
         * Returns a ByteBuffer with clean bytes from position to limit (exclusive).
         * Between invokes keeps previously returned bytes from mark to limit (exclusive),
         * but their place in memory and itself memory could be changed.
         *
         * @return a ByteBuffer which contains 4096 clean bytes for file reading.
         */
        @Override
        public ByteBuffer get() {
            final ByteBuffer memory = fetchMemory();
            if (tryAddCleanPage(memory) || tryCompact(memory)) {
                return memory;
            } else {
                return swapToTemporal(memory);
            }
        }

        private ByteBuffer fetchMemory() {
            return temporal == null ? general : trySwapToGeneral();
        }

        private boolean tryAddCleanPage(ByteBuffer memory) {
            if (memory.capacity() - memory.limit() >= PAGE_SIZE) {
                addCleanPage(memory, memory.limit());
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

        private void addCleanPage(ByteBuffer memory, int to) {
            memory.position(to).limit(to + PAGE_SIZE);
        }

        private void compress(ByteBuffer memory) {
            final int payload = memory.limit() - memory.position();

            memory.compact();
            prepareForRead(memory);
            addCleanPage(memory, payload);
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

        /**
         * Makes swap into a bigger memory region (usually is never invoked).
         * Exists for the worst use case: processing of a line
         * which is greater than 32768 characters.
         */
        private ByteBuffer swapToTemporal(ByteBuffer memory) {
            Logger.getLogger("MemoryAllocator").warning("Using additional memory!");

            final int payload = memory.capacity() - memory.position();

            final ByteBuffer target = ByteBuffer
                    .allocate(2 * memory.capacity())
                    .put(memory);

            prepareForRead(target);
            addCleanPage(target, payload);

            temporal = target;
            return target;
        }
    }
}
