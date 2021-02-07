package cacheread;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.odesk.agora.thrift.ds.directory.TOrganization;
import com.odesk.agora.thrift.ds.directory.TStaff;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TBase;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class Cacheread {
  public static void main(String[] args) throws IOException, InterruptedException {
    final TypeReference<List<TStaff>> typeRef = new TypeReference<List<TStaff>>() {
    };
    ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//    byte[] value = "x\\x9c}\\x93]O\\xc20\\x14\\x86\\xff\\x8a\\xe95\\x98u\\xc0$\\xdc\\x19\\x94Hbd\\xe1\\xc3\\x0b\\x8d\\x17\\xcdv\\x80&\\xb5%]\\x87A\\xc3\\x7f\\xb7\\xedF\\xdbMGzuN\\xcf{>\\x9e\\x9e\\xbe\\xff\\xa0\\x92\\xe6h\\x82\\xf0 \\xd6'\\x89\\x12\\x1c\\x8d\\xa2\\xd1(\\x19\\xe21\\xea\\xa1\\x03\\xc8Bp4\\xe1%c=$\\xe4\\x8ep\\xfaM\\x145\\xben\\xe5\\x9dQ\\x12\\t\\\\mLD%f\\xb0#\\xd9i\\xeel\\xd9\\xbeJ\\xadb\\xd9v\\xafO\\a\\xd0U^\\x81\\xe7B\\xea\\xc4\\x9c|\\x1a{&!\\xcf)\\xdc\\xcc\\x802Sn/\\x94\\xd8Hv\\xd1\\xaaJ\\xb5\\x12\\x0cR)\\x0e\\x92\\x82\\xb2\\xeaLpE\\xb2\\xb0J&\\xc1\\x0e\\xf4@\\x94Q\\xc4Q\\x1c\\xf51\\xeeG\\xf1\\x1a\\xc7\\x13\\x1cM\\x06\\xc9-\\x1e\\x8e\\xdf\\xb4V\\xeb\\xe8Q\\xc7l\\t+\\xa0\\x87\\xf64\\xcf\\x81;\\xb3\\x00U\\xb5\\xaadY\\x99\\xd3\\xa0\\x96\\x0f\\x9a6\\xea\\xb9\\xe0\\xfbfr\\xedy\\xfa\\x93\\xdf\\xd2t\\x8a\\xd4\\x13\\xf6!\\xcf\\x8e\\xb2\\xf7-\\xff\\x0b\\th\\xb7\\xafZc\\xbcX\\xe0\\xbe\\xac#mu\\xe7\\x9a\\x8b\\x9di\\xa5\\x88*\\x0b\\xb7._\\x1c\\xe4EI\\xf9\\x91*\\x1b\\x14\\xb2\\xd7\\x84\\xe4\\xb5\\xe7\\xa8|\\xd5\\x16n:\\xb6-Av3\\x83U\\x03\\x92\\xb9\\xa9\\xc8vK\\x19\\xd5\\xc9\\xa6u-\\xbd\\x06nb\\xa5o\\xabi/\\xfd0\\x06dWB\\xf7\\x83u\\xbe\\x86\\xef\\xd1\\xf9\\x16!\\x00c\\xd7]\\xfa\\x1ci\\xfd\\xbf\\xbcg\\xd1\\xf8c\\xcd\\xf5h0\\xf6\\x92y\\x13m\\xd0y\\x88\\xd7\\xbb\\x1f\\x03<&\\xf55B\\xa0V\\x1eR\\x98\\xb9\\t\\xea\\xfc\\xf1\\x0bQ4yj".getBytes();
//    final List<TStaff> tStaffs = mapper.<List<TStaff>>readValue(value, typeReference);


    String redisUrl = "redis://" + "collab-backend-prod-01.prod.cache.agora.odesk.com"
//    String redisUrl = "redis://" + "collab-backend.staging.cache.usw2.upwork"
        + ":" + 6379
        + "/" + "3";
    final RedisClient redisClient = RedisClient.create(redisUrl);

    RedisCodec codec = CompressionCodec.valueCompressor(new ByteArrayCodec(), CompressionCodec.CompressionType.DEFLATE);
    log.info("Connecting to redis");
    RedisReactiveCommands<byte[], byte[]> commands = redisClient.connect(codec).reactive();
    log.info("Connected to redis");
    commands.get("list_staff_1323608375771230208_null".getBytes())
        .map(value -> {
          try {
            final List<TStaff> tStaffs = mapper.readValue(value, typeRef);
            log.info("------------------------------------>>>>>>>>>>> Reading value from cache: {}", tStaffs.stream()
                .map(TStaff::getOrganization).map(TOrganization::getUid).collect(Collectors.toList()));
            return tStaffs;
          } catch (IOException e) {
            e.printStackTrace();
          }
          return 0;
        })
        .subscribe((o) -> {log.info("object");});


    Thread.sleep(1000000L);
  }



}