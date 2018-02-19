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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.Flow.Subscription;

import static java.lang.Long.MAX_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.write;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Zhytnik
 */
public class LineReaderTest {

    @ClassRule
    public static TemporaryFolder files = new TemporaryFolder();

    File file;
    LineReader reader;
    ReadAssertionSubscriber subscriber;

    @Before
    public void setUp() throws Exception {
        file = files.newFile();
        reader = new LineReader(file.toPath());
        subscriber = new ReadAssertionSubscriber();
    }

    @Test
    public void checksRequests() {
        subscriber.request = -1;
        reader.subscribe(subscriber.asExpected(IllegalArgumentException.class));

        assertThat(subscriber.isFailed()).isTrue();
    }

    @Test
    public void readsLazy() {
        file.delete();

        subscriber.doCancel = true;
        reader.subscribe(subscriber);
    }

    @Test
    public void processesInternalErrors() {
        file.delete();
        subscriber.request = MAX_VALUE;
        reader.subscribe(subscriber.asExpected(NoSuchFileException.class));

        assertThat(subscriber.isFailed()).isTrue();
    }

    @Test
    public void failsOnNotZeroRemainingLines() {
        writeToFile(
                '0', '1', '\r', '\n',
                '4', '\n',
                '7', '\r',
                '8'
        );

        subscriber.request = 5;
        reader.subscribe(subscriber.asExpected(LineReader.NoSuchLineCountException.class));

        assertThat(subscriber.isFailed()).isTrue();
    }

    @Test
    public void readsOnlyRequiredLinesAndReleasesResources() {
        writeToFile(
                '0', '1', '\r', '\n',
                '4', '\n',
                '7', '\r',
                '8'
        );

        subscriber.request = 3;
        reader.subscribe(subscriber);

        assertThat(file.delete()).isTrue();
        assertThat(subscriber.items).containsExactly("01", "4", "7");
    }

    @Test
    public void readsMultilineText() {
        readAll(
                '0', '1', '2', '3', '\n',
                '4', '5', '\r', '\n',
                '6', '7', '\r',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0123", "45", "67", "8");
    }

    @Test
    public void readsLinesWithDifferentEnds() {
        readAll(
                '0', '\r',
                '2', '\n',
                '6', '\r',
                '7', '\r', '\n',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0", "2", "6", "7", "8");
    }

    @Test
    public void readsEmptyFile() {
        readAll();

        assertThat(subscriber.items).isEmpty();
    }

    @Test
    public void readsSingleCharLine() {
        readAll('8');

        assertThat(subscriber.items).containsExactly("8");
    }

    @Test
    public void readsSingleLine() {
        readAll('4', '\r', '\n');

        assertThat(subscriber.items).containsExactly("4");
    }

    @Test
    public void readsRepeatableEmptyLines() {
        readAll(
                '0', '\n',
                '\r', '\n',
                '6', '\r',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0", "", "6", "8");
    }

    @Test
    public void readsTextWithEmptyLines() {
        readAll('\r', '\r');

        assertThat(subscriber.items).containsExactly("", "");
    }

    @After
    public void validate() {
        subscriber.validate();
    }

    void readAll(char... chars) {
        writeToFile(chars);
        subscriber.request = MAX_VALUE;
        reader.subscribe(subscriber);
    }

    void writeToFile(char... chars) {
        final byte[] bytes = new byte[chars.length];

        for (int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) chars[i];
        }
        try {
            write(file.toPath(), bytes, APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class ReadAssertionSubscriber extends BaseAssertionSubscriber<ByteBuffer, String> {

        boolean doCancel;

        @Override
        public void onSubscribe(Subscription s) {
            super.onSubscribe(s);
            if (doCancel) {
                unsubscribe();
            } else {
                doRequest();
            }
        }

        @Override
        public void onNext(ByteBuffer line) {
            super.onNext(line);
            items.add(UTF_8.decode(line).toString());
        }
    }
}