/*
 * Copyright 2018 Alexey Zhytnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zhytnik.reactive.io;

import com.zhytnik.reactive.io.LineReader.MemoryAllocator;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Zhytnik
 * @since 30-Jan-18
 */
public class MemoryAllocatorTest {

    MemoryAllocator allocator = new MemoryAllocator();

    @Test
    public void firstAllocatedMemoryHasMarkedStart() {
        assertThat(allocator.get().reset().position()).isEqualTo(0);
    }

    @Test
    public void alwaysAllocatesByPagesOf4KB() {
        final ByteBuffer memory = allocator.get();

        assertThat(memory.position() + 4096).isEqualTo(memory.limit());
    }

    @Test
    public void savesBytesFromMarkedPosition() {
        final ByteBuffer memory = allocator.get();

        memory.position(10).mark();

        memory.put(11, (byte) 1);
        memory.put(12, (byte) 2);
        memory.put(13, (byte) 3);

        memory.position(13).limit(13);

        final ByteBuffer nextPage = allocator.get();
        final int mark = nextPage.reset().position();

        assertThat(nextPage.get(mark + 1)).isEqualTo((byte) 1);
        assertThat(nextPage.get(mark + 2)).isEqualTo((byte) 2);
        assertThat(nextPage.get(mark + 3)).isEqualTo((byte) 3);
    }

    @Test
    public void compactsMemory() {
        assertThat(allocator.get().position()).isEqualTo(0);
        assertThat(allocator.get().position()).isEqualTo(4096);
        assertThat(allocator.get().position()).isEqualTo(2 * 4096);
        assertThat(allocator.get().position()).isEqualTo(3 * 4096);
        assertThat(allocator.get().position()).isEqualTo(4 * 4096);
        assertThat(allocator.get().position()).isEqualTo(5 * 4096);
        assertThat(allocator.get().position()).isEqualTo(6 * 4096);

        allocator.get().position(7 * 4096 + 4096 - 1).mark().put((byte) 7);

        final ByteBuffer compactedMemory = allocator.get();

        assertThat(compactedMemory.get(0)).isEqualTo((byte) 7);
        assertThat(compactedMemory.position()).isEqualTo(1);
        assertThat(compactedMemory.limit()).isEqualTo(1 + 4096);
    }

    @Test
    public void generalMemoryIsLimitedBy32KB() {
        final ByteBuffer general = allocator.get();

        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);
        assertThat(allocator.get()).isEqualTo(general);

        assertThat(allocator.get()).isNotEqualTo(general);
    }

    @Test
    public void triesToDoSwapWhenGeneralMemoryLimitIsReached() {
        allocator.get();

        allocator.get().put((byte) 77);

        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();

        final ByteBuffer swapped = allocator.get();

        assertThat(swapped.position()).isEqualTo(8 * 4096);
        assertThat(swapped.limit()).isEqualTo(8 * 4096 + 4096);
        assertThat(swapped.reset().position()).isEqualTo(0);
        assertThat(swapped.get(4096)).isEqualTo((byte) 77);
        assertThat(swapped.capacity()).isGreaterThanOrEqualTo(9 * 4096);
    }

    @Test
    public void alwaysTriesToUseGeneralMemory() {
        final ByteBuffer general = allocator.get();

        allocator
                .get()
                .position(4096 + 777).mark()
                .put((byte) 9);

        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();

        final ByteBuffer compacted = allocator.get();

        assertThat(compacted).isEqualTo(general);
        assertThat(compacted.position()).isEqualTo(8 * 4096 - 4096 - 777);
        assertThat(compacted.limit()).isEqualTo(8 * 4096 - 777);
        assertThat(compacted.reset().position()).isEqualTo(0);
        assertThat(compacted.get()).isEqualTo((byte) 9);

        final ByteBuffer temporal = allocator.get();

        assertThat(temporal).isNotEqualTo(general);
        temporal.position(temporal.limit() - 1).mark().put((byte) 7);

        final ByteBuffer swapped = allocator.get();

        assertThat(swapped).isEqualTo(general);
        assertThat(swapped.position()).isEqualTo(1);
        assertThat(swapped.limit()).isEqualTo(1 + 4096);
        assertThat(swapped.reset().position()).isEqualTo(0);
        assertThat(swapped.reset().get(0)).isEqualTo((byte) 7);
    }
}
