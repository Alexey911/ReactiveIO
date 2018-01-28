package com.zhytnik.reactive.examples;

import com.zhytnik.reactive.io.LineReader;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Alexey Zhytnik
 * @since 25.01.2018
 */
public class LineReadingExample {

    public static void main(String[] args) {
        final Path file = Paths.get("E://file2.txt");

        lines(file, 3, line ->
                System.out.println(UTF_8.decode(line))
        );
    }

    private static void lines(Path path, int count, Consumer<ByteBuffer> onNext) {
        new LineReader(path).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(count);
            }

            @Override
            public void onNext(ByteBuffer line) {
                onNext.accept(line);
            }

            @Override
            public void onError(Throwable e) {
                e.printStackTrace();
            }

            @Override
            public void onComplete() {
            }
        });
    }
}
