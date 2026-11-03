package com.hitachi.yx2602.heartbeat;

import java.util.List;

/** One batch succeeds only after its database transaction has committed. */
interface HeartbeatStore extends AutoCloseable {
    int write(List<HeartbeatRow> rows) throws Exception;
    @Override void close();
}
