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

    private static final int PAGE_SIZE = 4096;
    private static final int DIRECT_MEMORY_SIZE = 4 * PAGE_SIZE;

    private ByteBuffer heap;
    private final ByteBuffer direct;

    MemoryAllocator() {
        direct = ByteBuffer
                .allocateDirect(DIRECT_MEMORY_SIZE)
                .order(ByteOrder.nativeOrder())
                .limit(0)
                .mark();
    }

    @Override
    public ByteBuffer get() {
        final ByteBuffer memory = isDirectMode() ? direct : trySwapToDirect();

        if (memory.capacity() - memory.limit() >= PAGE_SIZE) {
            return addPage(memory, memory.limit());
        } else if (hasGarbage(memory)) {
            return compress(memory);
        } else {
            return swapToHeap(memory);
        }
    }

    private boolean isDirectMode() {
        return heap == null;
    }

    private ByteBuffer addPage(ByteBuffer memory, int to) {
        return memory.position(to).limit(to + PAGE_SIZE);
    }

    private boolean hasGarbage(ByteBuffer memory) {
        return memory.reset().position() >= PAGE_SIZE;
    }

    private ByteBuffer compress(ByteBuffer memory) {
        final int start = memory.position();

        memory.compact();
        prepareForRead(memory);
        addPage(memory, DIRECT_MEMORY_SIZE - start);
        return memory;
    }

    private void prepareForRead(ByteBuffer memory) {
        memory.position(0).mark();
    }

    private ByteBuffer trySwapToDirect() {
        if (heap.limit() - heap.reset().position() > (DIRECT_MEMORY_SIZE - PAGE_SIZE)) return heap;

        direct.position(0);
        direct.put(heap);
        prepareForRead(direct);
        direct.limit(heap.limit() - heap.position());

        heap = null;
        return direct;
    }

    private ByteBuffer swapToHeap(ByteBuffer memory) {
        final int payload = memory.capacity() - memory.position();

        final ByteBuffer target = ByteBuffer
                .allocate(2 * memory.capacity())
                .put(memory);

        prepareForRead(target);
        target.limit(payload + PAGE_SIZE);
        target.position(payload);

        heap = target;
        return target;
    }
}
