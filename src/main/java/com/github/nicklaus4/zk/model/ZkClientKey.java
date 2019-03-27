package com.github.nicklaus4.zk.model;

import java.util.Objects;

/**
 * zk client key
 *
 * @author weishibai
 * @date 2019/03/14 2:49 PM
 */
public class ZkClientKey {

    private String zkConnectStr;

    private String namespace;

    public String getZkConnectStr() {
        return zkConnectStr;
    }

    public void setZkConnectStr(String zkConnectStr) {
        this.zkConnectStr = zkConnectStr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public ZkClientKey(String zkConnectStr, String namespace) {
        this.zkConnectStr = zkConnectStr;
        this.namespace = namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ZkClientKey)) {
            return false;
        }
        ZkClientKey that = (ZkClientKey) o;
        return Objects.equals(zkConnectStr, that.zkConnectStr) //
                && Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zkConnectStr, namespace);
    }

}
