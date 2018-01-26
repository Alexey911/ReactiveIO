package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 26-Jan-18
 */
class MemoryAllocator implements Subscriber<ByteBuffer>, Supplier<ByteBuffer> {

    private ByteBuffer freeMemory;
    private Subscription memoryProvider;

    @Override
    public void onSubscribe(Subscription memoryProvider) {
        this.memoryProvider = memoryProvider;
    }

    @Override
    public void onNext(ByteBuffer memory) {
        freeMemory = memory;
    }

    @Override
    public void onError(Throwable memoryError) {
        throw new RuntimeException(memoryError);
    }

    @Override
    public void onComplete() {
        freeMemory = null;
        memoryProvider = null;
    }

    @Override
    public ByteBuffer get() {
        memoryProvider.request(1);
        return getExclusive();
    }

    private ByteBuffer getExclusive() {
        ByteBuffer memory = freeMemory;
        freeMemory = null;
        return memory;
    }
}
