package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 27-Jan-18
 */
class MemoryAllocator implements Supplier<ByteBuffer> {

    private static final int PAGE_SIZE = 2;

    private final ByteBuffer directMemory;

    MemoryAllocator() {
        ByteBuffer memory = ByteBuffer.allocateDirect(4 * PAGE_SIZE);
        memory.order(ByteOrder.nativeOrder());
        memory.limit(0);
        memory.mark();

        this.directMemory = memory;
    }

    @Override
    public ByteBuffer get() {
        final ByteBuffer memory = directMemory;

        if (memory.limit() == 0) {
            memory.limit(PAGE_SIZE);
        } else {
            final int limit = memory.limit();
            if (limit + PAGE_SIZE <= memory.capacity()) {
                memory.limit(limit + PAGE_SIZE);
            } else {
                int readStart = mark(memory);
                if (readStart + PAGE_SIZE <= memory.capacity()) {
                    int pos = memory.position();

                    memory.position(readStart);

                    memory.compact();
                    memory.position(0).mark();
                    memory.position(pos - readStart);
                    memory.limit(memory.position() + PAGE_SIZE);
                } else {
                    throw new UnsupportedOperationException("TODO: create use heap alternative");
                }
            }
        }

        return memory;
    }

    private static int mark(ByteBuffer b) {
        final int p = b.position();
        final int m = b.reset().position();
        b.position(p);
        return m;
    }
}
