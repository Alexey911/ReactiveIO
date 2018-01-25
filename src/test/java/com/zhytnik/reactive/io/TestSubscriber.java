package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
 * @since 24-Jan-18
 */
class TestSubscriber implements Subscriber<ByteBuffer>, Subscription {

    List<String> values = new ArrayList<>();

    @Override
    public void onSubscribe(Subscription s) {
    }

    @Override
    public void onNext(ByteBuffer item) {
        values.add(StandardCharsets.UTF_8.decode(item).toString());
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void onError(Throwable e) {
        throw new RuntimeException(e);
    }

    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
}
