package com.hitachi.yx2602.heartbeat;

/** 本次心跳的业务字段快照：缺失值保留 null，不填零，也不沿用上一条状态。 */
public class ElevatorState {
    public Long intId, fId;
    public String code;
    public Long floor, acd, direction, rs, doorLock;
    public Long isActive, isDebug, isRepair, isPower, isDoor, isHardware, isCall;
    public Long runTotal, runTime, speed;
    /** 故障编码使用字符串，保留“001”等编码中的前导零。 */
    public String mainFaultCode, subFaultCode;
    public Long doorCounter, secDoorCouter, weight;
    public Long tcda, tcdb, tcdc, tcdStopOutsideDoor, tcdOpenDoorRuning, tcdEntrapment, tcdDoor;
    public Long sendCount, receiveCount, signalRssi, networkType, recordStatus, status;
    public String createdDate, updatedDate;

    public ElevatorState() { }
}
