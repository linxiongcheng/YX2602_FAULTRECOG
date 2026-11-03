package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;

/** CreatedDate 统一按北京时间解析为毫秒时间戳，不依赖运行机器的默认时区。 */
public final class HeartbeatTime {
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu/M/d H:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 3, true).optionalEnd()
            .toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    private HeartbeatTime() { }

    static boolean missing(JsonNode value) {
        return value == null || value.isNull() || (value.isTextual() && value.textValue().trim().isEmpty());
    }

    public static long parseCreatedDate(JsonNode value) {
        if (missing(value) || !value.isTextual()) throw new IllegalArgumentException("CreatedDate必须为非空日期字符串");
        return parseCreatedDate(value.textValue());
    }

    public static long parseCreatedDate(String value) {
        if (value == null) throw new IllegalArgumentException("缺少CreatedDate");
        String text = value.trim();
        if (!text.matches("[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,3})?")) {
            throw new IllegalArgumentException("CreatedDate格式须为yyyy/M/d H:mm:ss，可选1～3位毫秒小数");
        }
        // 严格校验真实日期，并拒绝历史时区变更导致的不存在时间或歧义时间。
        LocalDateTime local = LocalDateTime.parse(text, FORMAT);
        List<ZoneOffset> offsets = ZONE.getRules().getValidOffsets(local);
        if (offsets.size() != 1) throw new IllegalArgumentException("CreatedDate处于北京时间历史时区跳变或歧义区间");
        long millis = local.toInstant(offsets.get(0)).toEpochMilli();
        if (millis <= 0) throw new IllegalArgumentException("CreatedDate须晚于Unix纪元");
        return millis;
    }

    /** SendTime 为可选元数据：仅接收正整数，缺失或非法返回 null，不决定数据有效性。 */
    static Long optionalSendTime(JsonNode value) {
        return value != null && value.isIntegralNumber() && value.canConvertToLong() && value.longValue() > 0
                ? value.longValue() : null;
    }
}
