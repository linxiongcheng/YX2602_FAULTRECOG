package com.hitachi.yx2602.heartbeat;

/** Nullable fields describe this heartbeat only; never fill unknown values with zero or old data. */
public class ElevatorState {
    public Long intId, fId;
    public String code;
    public Long floor, acd, direction, rs, doorLock;
    public Long isActive, isDebug, isRepair, isPower, isDoor, isHardware, isCall;
    public Long runTotal, runTime, speed;
    public String mainFaultCode, subFaultCode;
    public Long doorCounter, secDoorCouter, weight;
    public Long tcda, tcdb, tcdc, tcdStopOutsideDoor, tcdOpenDoorRuning, tcdEntrapment, tcdDoor;
    public Long sendCount, receiveCount, signalRssi, networkType, recordStatus, status;
    public String createdDate, updatedDate;

    public ElevatorState() { }
}
