package com.github.nicklaus4.zk.model;

/**
 * resource loader
 *
 * @author weishibai
 * @date 2019/03/27 11:28 AM
 */
@FunctionalInterface
public interface ResourceLoader<T> {

    T get();
}
