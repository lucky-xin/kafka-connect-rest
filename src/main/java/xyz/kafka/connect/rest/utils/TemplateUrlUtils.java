package xyz.kafka.connect.rest.utils;


import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TemplateUrlUtils
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class TemplateUrlUtils {
    private static final String REPLACEMENT_TOKEN = "confluentHttpSinkTemplateVariable";
    private static final String REGEX = "\\$\\{(.)*?}";
    private static final Pattern PATTERN = Pattern.compile(REGEX);
    private static final Set<String> NON_INTERESTING_TOKENS = Set.of("key", "topic");

    private TemplateUrlUtils() {
    }

    public static Set<String> getInterestingTemplateTokensFromUrl(String url) {
        if (StringUtils.isEmpty(url)) {
            return Collections.emptySet();
        }
        Set<String> set = new HashSet<>();
        Matcher m = PATTERN.matcher(url);
        while (m.find()) {
            String transformedToken = m.group()
                    .replace("$", "")
                    .replace("{", "")
                    .replace("}", "");
            if (!NON_INTERESTING_TOKENS.contains(transformedToken)) {
                set.add(transformedToken);
            }
        }
        return set;
    }

    public static String replaceAllTemplateTokensInUrl(String url) {
        return url.replaceAll(REGEX, REPLACEMENT_TOKEN);
    }

    public static boolean doesUrlContainInterestingTemplateTokens(String url) {
        return !getInterestingTemplateTokensFromUrl(url).isEmpty();
    }
}
