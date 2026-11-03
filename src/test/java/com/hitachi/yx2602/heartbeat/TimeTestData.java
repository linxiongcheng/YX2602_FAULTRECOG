package com.hitachi.yx2602.heartbeat;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

final class TimeTestData {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("uuuu/M/d H:mm:ss.SSS", Locale.ROOT)
            .withZone(ZoneId.of("Asia/Shanghai"));
    static String format(long millis) { return FORMAT.format(Instant.ofEpochMilli(millis)); }
    static String payload(long millis, String extra) {
        return "{\"CreatedDate\":\"" + format(millis) + "\"" + (extra.isEmpty() ? "" : "," + extra) + "}";
    }
}
