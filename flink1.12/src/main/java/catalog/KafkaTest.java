package catalog;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaTest {
    public static String CONTENT = "";

    @Test
    public void testSend() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.100.109:9092");// 服务器ip:端口号，集群用逗号分隔
        props.put("acks", "all");
        props.put("retries", 0);
//        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<>(props);


        ArrayList<String> strings = new ArrayList<>();

        strings.add("{\"id\":1111}");


//        strings.add("{\"id\":21}");
//        strings.add("{\"id\":31,\"age\":3,\"name\":\"xx13\"}");
//        strings.add("{\"id\":41,\"age\":4,\"name\":\"xx14\"}");
//        strings.add("{\"id\":51,\"age\":5,\"name\":\"xx15\"}");
//        strings.add("{\"id\":61,\"age\":6,\"name\":\"xx16\"}");
//        strings.add("{\"id\":71,\"age\":7,\"name\":\"xx17\"}");
//        strings.add("{\"id\":81,\"age\":8,\"name\":\"xx18\"}");
//        strings.add("{\"id\":19,\"age\":9,\"name\":\"xx19\"}");
//        strings.add("{\"id\":110,\"age\":10,\"name\":\"xx110\"}");
//        strings.add("{\"id\":111,\"age\":11,\"name\":\"xx111\"}");
//        strings.add("{\"id\":121,\"age\":12,\"name\":\"xx112\"}");

        int i = 23330;
        Gson gson = new Gson();
        while (true) {
            HashMap<String, Object> stringObjectHashMap = new HashMap<>();

            stringObjectHashMap.put("id", i++);
            stringObjectHashMap.put("name", "xxx" + i);
            String id =
                    gson.toJson(stringObjectHashMap);
//            for (String a1 : strings) {
            producer.send(new ProducerRecord<String, String>("leyouTopic", "", id), (a, b) -> {
                System.out.println(a);
            });

//            }
//            Thread.sleep(1000L);
        }

//        while (true) {
//        }
    }
}

