package com.zhytnik.reactive.io;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
 * @since 31.01.2018
 */
public abstract class BaseAssertionSubscriber<T, R> implements Subscriber<T> {

    private boolean used;
    private boolean failed;
    private boolean completed;
    private boolean subscribed;
    private boolean illegalUse;

    private boolean cancelled;
    private Class expectedError;
    private Subscription subscription;

    protected long request;
    protected List<R> items = new ArrayList<>();

    @Override
    public void onSubscribe(Subscription s) {
        if (!subscribed && !used && !failed && !completed) {
            subscription = s;
            subscribed = true;
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onSubscribe() call");
        }
    }

    @Override
    public void onNext(T item) {
        if (subscribed && !failed && !completed && !cancelled) {
            used = true;
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onNext() call");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onError(Throwable error) {
        if (!failed && !completed && !cancelled) {
            failed = true;

            if (expectedError == null || !expectedError.isAssignableFrom(error.getClass())) {
                expectedError = null;
                illegalUse = true;
                throw new IllegalStateException("Unexpected exception", error);
            } else {
                expectedError = null;
            }
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onError() call", error);
        }
    }

    @Override
    public void onComplete() {
        if (subscribed && !failed && !completed && !cancelled) {
            completed = true;
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onComplete() call");
        }
    }

    public Subscriber<T> asExpected(Class errorClass) {
        this.expectedError = errorClass;
        return this;
    }

    public void unsubscribe() {
        subscription.cancel();
        cancelled = true;
    }

    public void doRequest() {
        subscription.request(request);
    }

    public void validate() {
        if (illegalUse) {
            throw new RuntimeException("Illegal use was detected");
        }
        if (!subscribed && !used && !failed && !completed) {
            throw new IllegalStateException("Subscriber was unused");
        }
        if (!cancelled && subscribed && !failed && !completed) {
            throw new IllegalStateException("Subscriber wasn't closed");
        }
    }
}
