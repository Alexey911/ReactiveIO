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
    private static final int MEMORY_SIZE = 3 * PAGE_SIZE;

    private final ByteBuffer directMemory;

    private ByteBuffer temp;

    MemoryAllocator() {
        ByteBuffer memory = ByteBuffer.allocateDirect(MEMORY_SIZE);
        memory.order(ByteOrder.nativeOrder());
        memory.limit(0);

        reset(memory);

        this.directMemory = memory;
    }

    @Override
    public ByteBuffer get() {
        if (temp != null) {
            final ByteBuffer memory = temp;

            if (memory.limit() - memory.reset().position() <= PAGE_SIZE) {
                int payload = directMemory.position();
                reset(directMemory);

                directMemory.put(temp);

                extend(directMemory, payload);

                temp = null;
                return directMemory;
            } else {
                if (memory.limit() + PAGE_SIZE <= memory.capacity()) {
                    memory.limit(memory.limit() + PAGE_SIZE);
                } else {
                    throw new RuntimeException("TODO: how to avoid it!");
                }
            }
        }

        final ByteBuffer memory = directMemory;

        if (memory.limit() + PAGE_SIZE <= MEMORY_SIZE) {
            extend(memory, memory.limit());
        } else if (!tryCompress(memory)) {
            final int payload = MEMORY_SIZE - memory.position();

            ByteBuffer buffer = ByteBuffer.allocate(2 * MEMORY_SIZE).put(directMemory);

            buffer.position(0).mark();
            buffer.position(payload);
            buffer.limit(payload + PAGE_SIZE);

            return temp = buffer;
        }
        return memory;
    }

    private void reset(ByteBuffer memory) {
        memory.position(0).mark();
    }

    private void extend(ByteBuffer memory, int from) {
        memory.position(from).limit(from + PAGE_SIZE);
    }

    private boolean tryCompress(ByteBuffer memory) {
        final int start = memory.reset().position();

        if (start < PAGE_SIZE) return false;

        System.out.println("reusing " + (memory.limit() - start));

        memory.compact();
        reset(memory);
        extend(memory, MEMORY_SIZE - start);

        return true;
    }
}
