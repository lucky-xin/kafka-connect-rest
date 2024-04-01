package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSON;
import org.junit.jupiter.api.Test;
import xyz.kafka.connect.rest.sink.formatter.FastJsonWriter;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class JsonPathTests {

    @Test
    public void filter() throws Exception {
        Optional<String> op = Optional.empty();
        try (InputStream in = Files.newInputStream(Path.of("jms-error-log.json"))) {
            op = Optional.of(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        JSON.register(HashMap.class, new FastJsonWriter(op));
        Map<String, Object> map = new HashMap<>();
        map.put("service_name", "auth");
        map.put("@timestamp", DateTimeFormatter.ISO_DATE.format(LocalDateTime.now()));
        map.put("message", "query ids is empty. countryId:KHM, start:2017, end:2022, random id:null, exists:false, total:0");
        map.put("req_id", "ed71c0e65d354cda90ef1abb3326beb7");
        map.put("kibana_discover_url", "https://kiba.gzv.ink");
        List<Map<String, Object>> list = List.of(map, map);
        String json = JSON.toJSONString(list);
        System.out.println(json);

    }

    @Test
    public void test() {
        Pattern pattern = Pattern.compile("(?<hash>\\{.*\\})");
        Matcher matcher = pattern.matcher("hh{name}kk");
        if (matcher.find()) {
            System.out.println(matcher.group("hash"));
        }
    }
}
