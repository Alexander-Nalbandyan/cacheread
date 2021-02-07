package cacheread;

import org.joda.time.DateTime;
import  com.elanceodesk.workplace.collab.rooms.lib.Util;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Arrays;

public class CacheReadWithPipeline {
  private final static int ROOMS_CACHE_TTL = 24 * 3600;
  public static void main(String[] args) {

    final JedisPool jedisPool = Util.createJedisPool(
//        "collab-backend.staging.cache.usw2.upwork:6379", 2, 123123, "roomsWithStoriesPostedByUserCacheJedisPool");
        "collab-backend-dev-01.b0cv82.ng.0001.usw1.cache.amazonaws.com:6379", 2, 123123, "roomsWithStoriesPostedByUserCacheJedisPool");
    final Jedis jedis = jedisPool.getResource();
    jedis.select(41);
    Pipeline pipeline = jedis.pipelined();
    Long countResponse = null;
    try {
      String cacheKey = cacheKeyForRoomsWithStoriesPostedByUserCache("489501984089251840");
//      final String value = jedis.get(cacheKey);
//      System.out.println("value = " + value + ", for key " + cacheKey);
//      for (int i = 0; i < 17; i++) {
//        pipeline.sadd(cacheKey, "room_"+ i);// add roomId to the list
//      }
//
//      pipeline.expire(cacheKey, ROOMS_CACHE_TTL);
      countResponse = jedis.scard(cacheKey);

//      final String value = jedis.get("1339153448274649088");
//      System.out.println("value = " + value);
    } finally {
      pipeline.sync();
    }


    System.out.println("cacheKey = " + countResponse);
  }

  private static String cacheKeyForRoomsWithStoriesPostedByUserCache(String userId) {
    DateTime dateTime = new DateTime();
    return userId + "|" + dateTime.getMonthOfYear() + "-" + dateTime.getDayOfMonth();
  }
}
