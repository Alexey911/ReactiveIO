package com.zhytnik.reactive.examples;

import com.zhytnik.reactive.io.LineReader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
 * @since 25.01.2018
 */
public class LineReadingExample {

    public static void main(String[] args) {
        final Path path = Paths.get("E://file.txt");

        lines(path).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(3);
            }

            @Override
            public void onNext(ByteBuffer buffer) {
                System.out.println(StandardCharsets.UTF_8.decode(buffer));
            }

            @Override
            public void onError(Throwable e) {
                System.out.println("Exception occurred: " + e.getMessage());
            }

            @Override
            public void onComplete() {
            }
        });
    }

    private static Publisher<ByteBuffer> lines(Path path) {
        return new LineReader(path);
    }
}
