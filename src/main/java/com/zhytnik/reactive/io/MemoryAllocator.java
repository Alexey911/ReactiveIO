package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 27-Jan-18
 */
class MemoryAllocator implements Supplier<ByteBuffer> {

    private static final int PAGE_SIZE = 3;

    private ByteBuffer memory;

    @Override
    public ByteBuffer get() {
        if (memory == null) {
            allocate();
        } else {
            extend();
        }
        return memory;
    }

    private void allocate() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(10 * PAGE_SIZE);
        memory.order(ByteOrder.nativeOrder());
        memory.limit(PAGE_SIZE);
        memory.mark();

        this.memory = memory;
    }

    private void extend() {
        int prev = memory.limit();

        memory.limit(prev * 2);
        memory.position(prev);
        memory.mark();
    }

    public ByteBuffer getLastReleased() {
        return memory;
    }
}
