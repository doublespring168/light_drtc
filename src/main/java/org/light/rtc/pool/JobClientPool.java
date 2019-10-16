package org.light.rtc.pool;

import org.apache.thrift.transport.TTransport;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class JobClientPool {

    private ConcurrentHashMap<Integer, Vector<TTransport>> adminPool;
    private String adminIp;
    private int adminPort;

    private Vector<TTransport> jobPool;
    private String jobIp;
    private int jobPort;

}