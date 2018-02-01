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

    private Class expectedError;

    protected long request;
    protected List<R> items = new ArrayList<>();

    @Override
    public void onSubscribe(Subscription s) {
        if (!subscribed && !used && !failed && !completed) {
            subscribed = true;
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onSubscribe() call");
        }
    }

    @Override
    public void onNext(T item) {
        if (subscribed && !failed && !completed) {
            used = true;
        } else {
            illegalUse = true;
            throw new IllegalStateException("Unexpected onNext() call");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onError(Throwable error) {
        if (!failed && !completed) {
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
        if (subscribed && !failed && !completed) {
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

    public void validate() {
        if (illegalUse) throw new RuntimeException("Illegal behaviour was ignored");
        if (!subscribed && !used && !failed && !completed) throw new IllegalStateException("Subscriber was unused");
    }
}
