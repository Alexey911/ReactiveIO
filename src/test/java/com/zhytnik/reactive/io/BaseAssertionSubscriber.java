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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * @author Alexey Zhytnik
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
        if (subscribed && !failed && !completed && !cancelled) {
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

    public boolean isFailed() {
        return failed;
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
