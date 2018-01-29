package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 27-Jan-18
 */
//TODO: transfer to LineReader
class MemoryAllocator implements Supplier<ByteBuffer> {

    private static final int PAGE_SIZE = 2;
    private static final int MEMORY_SIZE = 4 * PAGE_SIZE;

    private final ByteBuffer directMemory;

    MemoryAllocator() {
        ByteBuffer memory = ByteBuffer.allocateDirect(MEMORY_SIZE);
        memory.order(ByteOrder.nativeOrder());
        memory.limit(0);

        reset(memory);

        this.directMemory = memory;
    }

    @Override
    public ByteBuffer get() {
        final ByteBuffer memory = directMemory;

        if (memory.limit() + PAGE_SIZE <= MEMORY_SIZE) {
            extend(memory, memory.limit());
        } else {
            compress(memory);
        }
        return memory;
    }

    private void reset(ByteBuffer memory) {
        memory.position(0).mark();
    }

    private void extend(ByteBuffer memory, int from) {
        memory.position(from).limit(from + PAGE_SIZE);
    }

    private void compress(ByteBuffer memory) {
        final int start = memory.reset().position();

        if (MEMORY_SIZE - start >= PAGE_SIZE) {
            memory.compact();
            reset(memory);
            extend(memory, MEMORY_SIZE - start);
        } else {
            throw new UnsupportedOperationException("TODO: use heap alternative");
        }
    }
}
