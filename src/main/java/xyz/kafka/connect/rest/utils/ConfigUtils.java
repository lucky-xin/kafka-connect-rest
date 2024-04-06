package xyz.kafka.connect.rest.utils;

import cn.hutool.core.text.StrPool;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.rangeClosed;

/**
 * @author luchaoxin
 */
public class ConfigUtils {


    private static Stream<String> breakDownList(String itemList) {
        if (itemList == null || itemList.isEmpty()) {
            return Stream.empty();
        }
        return Stream.of(itemList.split(StrPool.COMMA))
                .map(String::trim)
                .filter(it -> !it.isEmpty());
    }

    public static Set<Integer> parseIntegerRangedList(String rangedList) {
        return breakDownList(rangedList)
                .map(ConfigUtils::parseIntegerRange)
                .flatMap(Set::stream)
                .collect(toSet());
    }

    private static Set<Integer> parseIntegerRange(String range) {
        String[] rangeString = range.split("\\.\\.");
        if (rangeString.length == 0 || rangeString[0].isEmpty()) {
            return emptySet();
        } else if (rangeString.length == 1) {
            return asSet(Integer.valueOf(rangeString[0].trim()));
        } else if (rangeString.length == 2) {
            int from = Integer.parseInt(rangeString[0].trim());
            int to = Integer.parseInt(rangeString[1].trim());
            return (from < to ? rangeClosed(from, to) : rangeClosed(to, from)).boxed().collect(toSet());
        }
        throw new IllegalStateException(String.format("Invalid range definition %s", range));
    }

    private static Set<Integer> asSet(Integer... values) {
        return Stream.of(values).collect(toSet());
    }
}
