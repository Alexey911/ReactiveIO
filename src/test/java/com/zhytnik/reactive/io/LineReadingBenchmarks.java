package com.zhytnik.reactive.io;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("ALL")
public class LineReadingBenchmarks {

    @Fork(4)
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static abstract class BaseLineReadingBenchmark {

        @Param({})
        String path;

        Path nioPath;

        @Setup(Level.Trial)
        public void setUp() {
            nioPath = Paths.get(path);
        }
    }

    public static class LineReaderBenchmark extends BaseLineReadingBenchmark {

        @Benchmark
        public void read(Blackhole bh) {
            new LineReader(nioPath).subscribe(new LineSubscriber(bh));
        }

        @Benchmark
        public void lineCount(Blackhole bh) {
            new LineReader(nioPath).subscribe(new LineCountSubscriber(bh));
        }

        private static class LineSubscriber implements Subscriber<ByteBuffer> {

            private Blackhole blackhole;

            LineSubscriber(Blackhole blackhole) {
                this.blackhole = blackhole;
            }

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer line) {
                blackhole.consume(new String(line.array(), line.position(), line.remaining(), UTF_8));
            }

            @Override
            public void onError(Throwable e) {
                throw new RuntimeException(e);
            }

            @Override
            public void onComplete() {
            }
        }

        private static class LineCountSubscriber implements Subscriber<ByteBuffer> {

            private int count = 0;
            private Blackhole blackhole;

            LineCountSubscriber(Blackhole blackhole) {
                this.blackhole = blackhole;
            }

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer line) {
                ++count;
            }

            @Override
            public void onError(Throwable e) {
                throw new RuntimeException(e);
            }

            @Override
            public void onComplete() {
                blackhole.consume(count);
            }
        }
    }

    public static class DefaultReadBenchmark extends BaseLineReadingBenchmark {

        @Benchmark
        public void read(Blackhole bh) throws IOException {
            try (Stream<String> lines = Files.lines(nioPath, UTF_8)) {
                lines.forEach(bh::consume);
            }
        }

        @Benchmark
        public void parallelRead(Blackhole bh) throws IOException {
            try (Stream<String> lines = Files.lines(nioPath, UTF_8).parallel()) {
                lines.forEach(bh::consume);
            }
        }

        @Benchmark
        public void lineCount(Blackhole bh) throws IOException {
            try (Stream<String> lines = Files.lines(nioPath, UTF_8)) {
                bh.consume(lines.count());
            }
        }

        @Benchmark
        public void parallelLineCount(Blackhole bh) throws IOException {
            try (Stream<String> lines = Files.lines(nioPath, UTF_8).parallel()) {
                bh.consume(lines.count());
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LineReadingBenchmarks.class.getSimpleName())
                .param("path", System.getProperty("path"))
                .build();

        new Runner(opt).run();
    }
}
