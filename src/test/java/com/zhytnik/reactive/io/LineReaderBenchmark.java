package com.zhytnik.reactive.io;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;

/**
 * @author Alexey Zhytnik
 * @since 24.01.2018
 */
@Fork(4)
@Warmup(iterations = 24, time = 1)
@Measurement(iterations = 4)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
public class LineReaderBenchmark {

    @Param({})
    String path;

    LineReader reader;
    BenchmarkSubscriber subscriber;

    @Setup(Level.Trial)
    public void setUp() {
        reader = new LineReader(Paths.get(path));
        subscriber = new BenchmarkSubscriber();
    }

    @Benchmark
    public void read(Blackhole bh) {
        subscriber.blackhole = bh;
        reader.subscribe(subscriber);
    }

    private static class BenchmarkSubscriber implements Subscriber<ByteBuffer> {

        private Blackhole blackhole;

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer line) {
            blackhole.consume(line.remaining());
        }

        @Override
        public void onError(Throwable e) {
            throw new RuntimeException(e);
        }

        @Override
        public void onComplete() {
        }
    }

    public static void main(String[] args) throws RunnerException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Please, set up file path for benchmarking!");
        }

        Options opt = new OptionsBuilder()
                .include(LineReaderBenchmark.class.getSimpleName())
                .param("path", args[0])
                .build();

        new Runner(opt).run();
    }
}
