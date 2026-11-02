package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParserStateTest {
    private static final long NOW = HeartbeatSplitterTest.NOW;

    private KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AcceptNewerState(600000)),
                update -> update.elevatorId, Types.LONG, 128, 1, 0);
    }

    private ElevatorUpdate update(long id, long time) {
        ElevatorUpdate update = new ElevatorUpdate();
        update.elevatorId = id;
        update.sendTimeMs = time;
        update.state = new ElevatorState();
        return update;
    }

    @Test void splitExpiryBoundaryAndExpiredBadFieldsDoNotGoToErrors() throws Exception {
        try (OneInputStreamOperatorTestHarness<HeartbeatMessage, ElevatorUpdate> harness =
                     new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new SplitHeartbeat(600000)))) {
            harness.open();
            harness.setProcessingTime(NOW);
            harness.processElement(new StreamRecord<>(HeartbeatSplitterTest.message(NOW - 600000, "\"EId1\":11")));
            harness.processElement(new StreamRecord<>(HeartbeatSplitterTest.message(NOW - 600001,
                    "\"EId1\":11,\"Speed1\":\"bad\"")));
            assertEquals(1, harness.extractOutputValues().size());
            assertNull(harness.getSideOutput(SplitHeartbeat.ERRORS));
        }
    }

    @Test void onlyAcceptedNewerRecordsAdvanceTheirOwnSequence() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> harness = harness()) {
            harness.open();
            harness.setProcessingTime(NOW);
            harness.processElement(new StreamRecord<>(update(11, NOW - 1000)));
            harness.processElement(new StreamRecord<>(update(11, NOW - 1000)));
            harness.processElement(new StreamRecord<>(update(11, NOW - 2000)));
            harness.processElement(new StreamRecord<>(update(33, NOW - 3000)));
            harness.processElement(new StreamRecord<>(update(11, NOW)));
            List<ElevatorUpdate> results = harness.extractOutputValues();
            assertEquals(3, results.size());
            assertEquals(1, results.get(0).historySequence);
            assertEquals(1, results.get(1).historySequence);
            assertEquals(2, results.get(2).historySequence);
            assertEquals(11, results.get(2).elevatorId);
            assertEquals(NOW, results.get(2).parsedAtMs);
        }
    }

    @Test void lateArrivalAfterShuffleIsDroppedAndDoesNotCreateState() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> harness = harness()) {
            harness.open();
            harness.setProcessingTime(NOW);
            harness.processElement(new StreamRecord<>(update(11, NOW - 600001)));
            harness.processElement(new StreamRecord<>(update(11, NOW - 600000)));
            assertEquals(1, harness.extractOutputValues().size());
            assertEquals(1, harness.extractOutputValues().get(0).historySequence);
        }
    }

    @Test void checkpointRestoresTimeAndSequenceEvenAfterLongOfflinePeriod() throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> first = harness()) {
            first.open();
            first.setProcessingTime(NOW);
            first.processElement(new StreamRecord<>(update(11, NOW - 1000)));
            first.processElement(new StreamRecord<>(update(11, NOW)));
            snapshot = first.snapshot(1, NOW);
        }
        try (KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.setProcessingTime(NOW + 100);
            restored.processElement(new StreamRecord<>(update(11, NOW)));
            restored.processElement(new StreamRecord<>(update(11, NOW - 500)));
            assertTrue(restored.extractOutputValues().isEmpty());
            long daysLater = NOW + 45L * 24 * 60 * 60 * 1000;
            restored.setProcessingTime(daysLater);
            restored.setStateTtlProcessingTime(daysLater);
            restored.processElement(new StreamRecord<>(update(11, NOW + 1000))); // Offline backlog.
            restored.processElement(new StreamRecord<>(update(11, daysLater))); // Fresh after reconnect.
            assertEquals(1, restored.extractOutputValues().size());
            assertEquals(3, restored.extractOutputValues().get(0).historySequence);
        }
    }

    @Test void newSnapshotDoesNotCarryForwardOldNonNullFields() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, ElevatorUpdate, ElevatorUpdate> harness = harness()) {
            harness.open();
            harness.setProcessingTime(NOW);
            ElevatorUpdate first = update(11, NOW - 100);
            first.state.speed = 98L;
            harness.processElement(new StreamRecord<>(first));
            harness.processElement(new StreamRecord<>(update(11, NOW)));
            assertNull(harness.extractOutputValues().get(1).state.speed);
        }
    }
}
