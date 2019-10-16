package org.light.rtc.dao;

import org.light.rtc.util.Constants;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class RedisDao {
    private static RedisDao instance;

    private ShardedJedis jedis;
//	private Jedis jedis;

    private RedisDao() {
        this.init();
    }

    public void init() {
        String[] redisList = Constants.redisHosts.split(",");
        ArrayList<JedisShardInfo> userMasterWriteJedisShardInfoList = new ArrayList<JedisShardInfo>();
        for (int i = 0; i < redisList.length; i++) {
            String[] hostPorts = redisList[i].split(":");
            JedisShardInfo userMasterWriteJedis = null;
            try {
                userMasterWriteJedis = new JedisShardInfo(InetAddress.getByName(hostPorts[0]).getHostAddress(), Integer.parseInt(hostPorts[1]), 10000);
                userMasterWriteJedis.setPassword(Constants.redisPswd);
            } catch (UnknownHostException e) {
            }
            userMasterWriteJedisShardInfoList.add(userMasterWriteJedis);
        }
        this.jedis = new ShardedJedis(userMasterWriteJedisShardInfoList, ShardedJedis.DEFAULT_KEY_TAG_PATTERN);

//        String[] hostIp = Constants.redisHost.split(":");
//		this.jedis = new Jedis(hostIp[0], Integer.parseInt(hostIp[1]),10000);
//		this.jedis.auth(Constants.redisPswd);
    }

    public synchronized static RedisDao getInstance() {
        if (instance == null) {
            instance = new RedisDao();
        }
        return instance;
    }

    public static void main(String[] args) {
        RedisDao rdao = new RedisDao();
        rdao.test();
    }

    public void test() {

    }

    public void addStreamLogToList(List<String> minBathLogs) {

    }

}
