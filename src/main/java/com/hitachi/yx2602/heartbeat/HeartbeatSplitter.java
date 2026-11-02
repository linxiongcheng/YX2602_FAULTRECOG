package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Stateless field parsing. A bad port cannot discard the other elevators in the same message. */
public final class HeartbeatSplitter {
    private final ObjectMapper mapper = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(100).build()).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private static final List<NumberField> PORT_FIELDS = new ArrayList<>();
    private static final List<NumberField> COMMON_FIELDS = new ArrayList<>();
    static {
        port("Floor", Integer.MIN_VALUE, Integer.MAX_VALUE, (s, v) -> s.floor = v);
        port("ACD", 0, Integer.MAX_VALUE, (s, v) -> s.acd = v);
        port("Direction", 0, Integer.MAX_VALUE, (s, v) -> s.direction = v);
        port("RS", 0, 1, (s, v) -> s.rs = v);
        port("DoorLock", 0, 1, (s, v) -> s.doorLock = v);
        port("IsActive", 0, 1, (s, v) -> s.isActive = v);
        port("IsDebug", 0, 1, (s, v) -> s.isDebug = v);
        port("IsRepair", 0, 1, (s, v) -> s.isRepair = v);
        port("IsPower", 0, 1, (s, v) -> s.isPower = v);
        port("IsDoor", 0, 1, (s, v) -> s.isDoor = v);
        port("IsHardware", 0, 1, (s, v) -> s.isHardware = v);
        port("IsCall", 0, 1, (s, v) -> s.isCall = v);
        port("RunTotal", 0, Long.MAX_VALUE, (s, v) -> s.runTotal = v);
        port("RunTime", 0, Long.MAX_VALUE, (s, v) -> s.runTime = v);
        port("Speed", 0, Long.MAX_VALUE, (s, v) -> s.speed = v);
        port("DoorCounter", 0, Long.MAX_VALUE, (s, v) -> s.doorCounter = v);
        // Preserve the upstream spelling in the input contract.
        port("SecDoorCouter", 0, Long.MAX_VALUE, (s, v) -> s.secDoorCouter = v);
        port("Weight", 0, Long.MAX_VALUE, (s, v) -> s.weight = v);
        port("TCDA", 0, 1, (s, v) -> s.tcda = v);
        port("TCDB", 0, 1, (s, v) -> s.tcdb = v);
        port("TCDC", 0, 1, (s, v) -> s.tcdc = v);
        port("TCDStopOutsideDoor", 0, 1, (s, v) -> s.tcdStopOutsideDoor = v);
        port("TCDOpenDoorRuning", 0, 1, (s, v) -> s.tcdOpenDoorRuning = v);
        port("TCDEntrapment", 0, 1, (s, v) -> s.tcdEntrapment = v);
        port("TCDDoor", 0, 1, (s, v) -> s.tcdDoor = v);
        common("IntId", 0, Long.MAX_VALUE, (s, v) -> s.intId = v);
        common("FId", 0, Long.MAX_VALUE, (s, v) -> s.fId = v);
        common("SendCount", 0, Long.MAX_VALUE, (s, v) -> s.sendCount = v);
        common("ReceiveCount", 0, Long.MAX_VALUE, (s, v) -> s.receiveCount = v);
        common("SignalRssi", 0, 31, (s, v) -> s.signalRssi = v);
        common("NetworkType", 1, 2, (s, v) -> s.networkType = v);
        common("RecordStatus", Integer.MIN_VALUE, Integer.MAX_VALUE, (s, v) -> s.recordStatus = v);
        common("Status", Integer.MIN_VALUE, Integer.MAX_VALUE, (s, v) -> s.status = v);
    }

    public Result split(HeartbeatMessage message, long nowMs) {
        Result result = new Result();
        JsonNode root;
        try { root = mapper.readTree(message.payloadJson); }
        catch (IOException | IllegalArgumentException e) {
            result.errors.add(error(message, 0, null, "INVALID_JSON", "$", "无法解析JSON对象", nowMs));
            return result;
        }
        if (root == null || !root.isObject()) {
            result.errors.add(error(message, 0, null, "INVALID_STRUCTURE", "$", "根节点必须是对象", nowMs));
            return result;
        }
        JsonNode sendTime = root.get("SendTime");
        if (sendTime == null || !sendTime.isIntegralNumber() || !sendTime.canConvertToLong()
                || sendTime.longValue() <= 0 || sendTime.longValue() != message.sendTimeMs) {
            result.errors.add(error(message, 0, null, "INVALID_SEND_TIME", "SendTime",
                    "发送时间必须与读取模块输出的正整数毫秒时间戳一致", nowMs));
            return result;
        }
        Map<Integer, Long> ids = new HashMap<>();
        Map<Long, Integer> occurrences = new HashMap<>();
        int present = 0;
        for (int port = 1; port <= 3; port++) {
            if (!portPresent(root, port)) {
                result.absentPorts++;
                continue;
            }
            present++;
            try {
                Long id = integer(root.get("EId" + port), "EId" + port, 0, Long.MAX_VALUE);
                if (id == null) throw new FieldError("EId" + port, "非空端口必须提供有效电梯编号");
                if (id == 0) { result.emptyPorts++; continue; }
                ids.put(port, id);
                occurrences.put(id, occurrences.getOrDefault(id, 0) + 1);
            } catch (FieldError e) {
                result.errors.add(error(message, port, null, "INVALID_ELEVATOR_ID", e.field, e.getMessage(), nowMs));
            }
        }
        if (present == 0) {
            result.errors.add(error(message, 0, null, "NO_ELEVATOR_DATA", "EId1/EId2/EId3",
                    "心跳中没有可识别的电梯端口数据", nowMs));
        }
        for (int port = 1; port <= 3; port++) {
            Long id = ids.get(port);
            if (id == null) continue;
            if (occurrences.get(id) > 1) {
                result.errors.add(error(message, port, id, "DUPLICATE_ELEVATOR_ID", "EId" + port,
                        "同一心跳多个端口使用相同电梯编号，无法确定有效状态", nowMs));
                continue;
            }
            try {
                ElevatorState state = new ElevatorState();
                for (NumberField field : COMMON_FIELDS) field.read(root, "", state);
                for (NumberField field : PORT_FIELDS) field.read(root, Integer.toString(port), state);
                state.code = code(root.get("Code"), "Code");
                state.mainFaultCode = code(root.get("MainFaultCode" + port), "MainFaultCode" + port);
                state.subFaultCode = code(root.get("SubFaultCode" + port), "SubFaultCode" + port);
                state.createdDate = text(root.get("CreatedDate"), "CreatedDate");
                state.updatedDate = text(root.get("UpdatedDate"), "UpdatedDate");
                result.valid.add(ElevatorUpdate.from(message, port, id, state));
            } catch (FieldError e) {
                result.errors.add(error(message, port, id, "INVALID_FIELD", e.field, e.getMessage(), nowMs));
            }
        }
        return result;
    }

    public static boolean expired(long sendTimeMs, long nowMs, long maxAgeMs) {
        return nowMs > sendTimeMs && nowMs - sendTimeMs > maxAgeMs;
    }

    private static boolean portPresent(JsonNode root, int port) {
        if (root.has("EId" + port) || root.has("MainFaultCode" + port) || root.has("SubFaultCode" + port)) return true;
        for (NumberField field : PORT_FIELDS) if (root.has(field.name + port)) return true;
        return false;
    }

    private static Long integer(JsonNode node, String field, long min, long max) throws FieldError {
        if (node == null || node.isNull()) return null;
        long value;
        if (node.isIntegralNumber() && node.canConvertToLong()) value = node.longValue();
        else if (node.isTextual() && node.textValue().trim().matches("[+-]?[0-9]+")) {
            try { value = Long.parseLong(node.textValue().trim()); }
            catch (NumberFormatException e) { throw new FieldError(field, "整数超出64位范围"); }
        } else throw new FieldError(field, "必须是整数或合法整数字符串");
        if (value < min || value > max) throw new FieldError(field, "取值须在[" + min + "," + max + "]内");
        return value;
    }

    private static String code(JsonNode node, String field) throws FieldError {
        if (node == null || node.isNull()) return null;
        if (node.isTextual() && !node.textValue().trim().isEmpty()) return node.textValue();
        if (node.isIntegralNumber() && node.canConvertToLong() && node.longValue() >= 0) return node.asText();
        throw new FieldError(field, "编码必须为非空字符串或非负64位整数");
    }

    private static String text(JsonNode node, String field) throws FieldError {
        if (node == null || node.isNull()) return null;
        if (node.isTextual()) return node.textValue();
        throw new FieldError(field, "必须为字符串；辅助时间字段不参与排序");
    }

    private String error(HeartbeatMessage message, int port, Long id, String code,
                         String field, String reason, long nowMs) {
        ObjectNode error = mapper.createObjectNode();
        error.put("schema_version", 1);
        error.put("stage", "parsing");
        error.put("message_id", message.sourceTopic + ":" + message.sourcePartition + ":"
                + message.sourceOffset + ":port:" + port + ":" + code);
        error.put("port", port);
        if (id == null) error.putNull("elevator_id"); else error.put("elevator_id", id);
        error.put("error_code", code);
        error.put("error_field", field);
        error.put("error_reason", reason);
        error.put("processed_at_ms", nowMs);
        ObjectNode source = error.putObject("source");
        source.put("topic", message.sourceTopic);
        source.put("partition", message.sourcePartition);
        source.put("offset", message.sourceOffset);
        source.put("kafka_timestamp_ms", message.kafkaTimestampMs);
        error.put("raw_value_base64", Base64.getEncoder().encodeToString(
                message.payloadJson.getBytes(StandardCharsets.UTF_8)));
        return error.toString();
    }

    private static void port(String name, long min, long max, BiConsumer<ElevatorState, Long> setter) {
        PORT_FIELDS.add(new NumberField(name, min, max, setter));
    }

    private static void common(String name, long min, long max, BiConsumer<ElevatorState, Long> setter) {
        COMMON_FIELDS.add(new NumberField(name, min, max, setter));
    }

    private static final class NumberField {
        final String name;
        final long min, max;
        final BiConsumer<ElevatorState, Long> setter;
        NumberField(String name, long min, long max, BiConsumer<ElevatorState, Long> setter) {
            this.name = name; this.min = min; this.max = max; this.setter = setter;
        }
        void read(JsonNode root, String suffix, ElevatorState state) throws FieldError {
            setter.accept(state, integer(root.get(name + suffix), name + suffix, min, max));
        }
    }

    private static final class FieldError extends Exception {
        final String field;
        FieldError(String field, String reason) { super(reason); this.field = field; }
    }

    public static final class Result {
        public final List<ElevatorUpdate> valid = new ArrayList<>();
        public final List<String> errors = new ArrayList<>();
        public int emptyPorts;
        public int absentPorts;
    }
}
