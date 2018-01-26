package com.zhytnik.reactive.io;

import com.zhytnik.reactive.io.LineReader.LineParser;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Zhytnik
 * @since 25.01.2018
 */
public class LineReaderTest {

    LineParser reader;
    TestSubscriber subscriber;

    @Before
    public void setUp() {
        subscriber = new TestSubscriber();

        final LineReader.LineReadingSubscription s = new LineReader.LineReadingSubscription();
        s.request(Long.MAX_VALUE);

        reader = new LineParser(subscriber, s);
        reader.onSubscribe(subscriber);
    }

    @Test
    public void stressTest() {
        reader.onNext(bytes(
                '0', '1', '2', '3', '\n',
                '4', '5', '\r', '\n',
                '6', '7', '\r',
                '8'
        ));

        assertThat(subscriber.values).containsSequence("0123", "45", "67", "8");
    }

    @Test
    public void parsesDifferentLineEnds() {
        reader.onNext(bytes(
                '0', '\r',
                '2', '\n',
                '6', '\r',
                '7', '\r', '\n',
                '8'
        ));

        assertThat(subscriber.values).containsSequence("0", "2", "6", "7", "8");
    }

    @Test
    public void parsesEmptyLine() {
        reader.onNext(bytes());

        assertThat(subscriber.values).containsSequence();
    }

    @Test
    public void parsesSingleCharLine() {
        reader.onNext(bytes('8'));

        assertThat(subscriber.values).containsSequence("8");
    }

    @Test
    public void parsesSingleLine() {
        reader.onNext(bytes('4', '\r', '\n'));

        assertThat(subscriber.values).containsSequence("4");
    }

    @Test
    public void parsesFewContinuousEmptyLines() {
        reader.onNext(bytes(
                '0', '\n',
                '\r', '\n',
                '6', '\r',
                '8'
        ));

        assertThat(subscriber.values).containsSequence("0", "", "6", "8");
    }

    @Test
    public void parsesRepeatableLines() {
        reader.onNext(bytes(
                '0', '\n',
                '1', '\n'
        ));

        assertThat(subscriber.values).containsSequence("0", "1");
    }

    ByteBuffer bytes(char... vals) {
        final byte[] out = new byte[vals.length];

        for (int i = 0; i < vals.length; i++) out[i] = (byte) vals[i];

        return ByteBuffer.wrap(out);
    }
}