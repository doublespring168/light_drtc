package org.light.rtc.admin;

import org.light.rtc.client.LrtdcClient;
import org.light.rtc.util.ConfigProperty;
import org.light.rtc.util.Constants;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class AdminNodeService {
    private final SimpleDateFormat timeNumSdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private String[] jobNodeHosts = Constants.jobNodeHosts.split(",");
    private int jobNodeNum = jobNodeHosts.length;
    private ConcurrentHashMap<Integer, LrtdcClient> jobNodeClientMap;
    private List<String> userActions;
    private Random rand = new Random();

    public AdminNodeService() {
        this.init();
    }

    private void init() {
        if (jobNodeHosts != null) {
            jobNodeClientMap = new ConcurrentHashMap<Integer, LrtdcClient>();
            for (int i = 0; i < jobNodeNum; i++) {
                String[] tmpHostIp = jobNodeHosts[i].split(":");
                jobNodeClientMap.put(i, new LrtdcClient(tmpHostIp[0], Integer.parseInt(tmpHostIp[1])));
            }
        }
    }

    public void setUserActions(List<String> uLogs) {
        this.userActions = uLogs;
    }

    public long getCurrentTime() {
        return Long.parseLong(timeNumSdf.format(new Date()));
    }

    public void run() {
        long begin = 0, end = 0;
        int uidNum = userActions.size();
        if (uidNum > 0) {
            System.out.println(ConfigProperty.getCurDateTime() + " 获取最近待执行 " + Constants.rtcPeriodSeconds + " 秒 用户行为数据共有 " + userActions.size());
            Map<Integer, Integer> rtMap = new HashMap<Integer, Integer>();
            int adminNodeId = -1;
            int rtResult = -1;
            List<Integer> enableJobIds = this.getAbleJobNodeIds();
            int enableServerNum = enableJobIds.size();
            if (uidNum < Constants.minJobBatchNum) {
                adminNodeId = enableJobIds.get(rand.nextInt(enableServerNum));
                if (jobNodeClientMap.get(adminNodeId).getHealthStatus() == 1) {
                    rtResult = jobNodeClientMap.get(adminNodeId).getRtcStatsResult(userActions);
                    if (rtResult < 1) {
                        rtResult = jobNodeClientMap.get(adminNodeId).getRtcStatsResult(userActions);
                    }
                }
                rtMap.put(adminNodeId, rtResult);
            } else {
                List<Future<Map<Integer, Integer>>> futList = new LinkedList<Future<Map<Integer, Integer>>>();
                ExecutorService exPool = Executors.newFixedThreadPool(enableServerNum);
                int eachJobUserNum = (int) Math.ceil(1.0 * userActions.size() / enableServerNum);
                int startCur = 0, endCur = 0;
                for (int j = 0; j < enableServerNum; j++) {
                    startCur = j * eachJobUserNum;
                    if (j < enableServerNum - 1) {
                        endCur = (j + 1) * eachJobUserNum;
                        if (endCur > uidNum) {
                            endCur = uidNum;
                        }
                        if (startCur < endCur) {
                            futList.add(exPool.submit(new StatsJob(userActions.subList(startCur, endCur), j)));
                        }
                    } else {
                        if (startCur < uidNum) {
                            futList.add(exPool.submit(new StatsJob(userActions.subList(startCur, uidNum), j)));
                        }
                    }
                }
                try {
                    for (Future<Map<Integer, Integer>> fut : futList) {
                        rtMap.putAll(fut.get());
                    }
                    futList.clear();
                    futList = null;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                if (!exPool.isShutdown()) {
                    exPool.shutdown();
                }
                exPool = null;
                this.reRun(rtMap, eachJobUserNum, uidNum, enableServerNum);
            }
            end = System.currentTimeMillis();
            System.out.println(ConfigProperty.getCurDateTime() + " 最近待执行的  " + Constants.rtcPeriodSeconds + " 秒 用户短期兴趣标签共耗时： " + (double) (end - begin) / 1000 + " 秒 " + rtMap);

        }

    }

    public List<Integer> getAbleJobNodeIds() {
        List<Integer> ableJobNodeIds = new ArrayList<>();
        for (Entry<Integer, LrtdcClient> item : jobNodeClientMap.entrySet()) {
            if (item.getValue().getHealthStatus() == 1) {
                ableJobNodeIds.add(item.getKey());
            }
        }
        return ableJobNodeIds;
    }

    /**
     * check job result is ok, if failure, reRun this job
     *
     * @param rtMap
     * @param eachJobUserNum
     * @param uidNum
     */
    public void reRun(Map<Integer, Integer> rtMap, int eachJobUserNum, int uidNum, int enableServerNum) {
        int startCur = 0, endCur = 0;
        for (Entry<Integer, Integer> item : rtMap.entrySet()) {
            if (item.getValue() < 1) {
                startCur = item.getKey() * eachJobUserNum;
                if (item.getKey() < enableServerNum - 1) {
                    endCur = (item.getKey() + 1) * eachJobUserNum;
                    if (endCur > uidNum) {
                        endCur = uidNum;
                    }
                    if (startCur < endCur) {
                        rtMap.put(item.getKey(), jobNodeClientMap.get(item.getKey()).getRtcStatsResult(userActions.subList(startCur, endCur)));
                    }
                } else {
                    if (startCur < uidNum) {
                        rtMap.put(item.getKey(), jobNodeClientMap.get(item.getKey()).getRtcStatsResult(userActions.subList(startCur, uidNum)));
                    }
                }
            }
        }
    }

    protected class StatsJob implements Callable<Map<Integer, Integer>> {

        public List<String> userNids;
        public int nodeId;

        public StatsJob(List<String> userNids, int jobNodeId) {
            this.userNids = userNids;
            this.nodeId = jobNodeId;
        }

        @Override
        public Map<Integer, Integer> call() throws Exception {
            Map<Integer, Integer> rtMap = new HashMap<Integer, Integer>();
            if (jobNodeClientMap.get(this.nodeId).getHealthStatus() == 1) {
                int rtResult = jobNodeClientMap.get(this.nodeId).getRtcStatsResult(userNids);
                if (rtResult < 1) {
                    rtResult = jobNodeClientMap.get(this.nodeId).getRtcStatsResult(userNids);
                }
                rtMap.put(this.nodeId, rtResult);
            }
            return rtMap;
        }

    }

//	public static void main(String[] args) {
//		AdminNodeService t = new AdminNodeService();
//
//	}

}
