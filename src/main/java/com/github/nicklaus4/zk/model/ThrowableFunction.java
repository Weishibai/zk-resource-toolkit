package com.github.nicklaus4.zk.model;

import static java.util.Objects.requireNonNull;

/**
 * throwable func
 *
 * @author weishibai
 * @date 2019/03/27 11:17 AM
 */
@FunctionalInterface
public interface ThrowableFunction<T, R, X extends Throwable> {

    static <T, X extends Throwable> ThrowableFunction<T, T, X> identity() {
        return t -> t;
    }

    R apply(T t) throws X;

    default <V> ThrowableFunction<V, R, X> compose(ThrowableFunction<? super V, ? extends T, X> before) {
        requireNonNull(before);
        return (V v) -> apply(before.apply(v));
    }

    default <V> ThrowableFunction<T, V, X> andThen(ThrowableFunction<? super R, ? extends V, X> after) {
        requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }
}
