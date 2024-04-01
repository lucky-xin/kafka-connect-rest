package xyz.kafka.connect.rest.utils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;


/**
 * UriUtils
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public final class UriUtils {
    private static final Set<Character> SUB_DELIMITERS = Set.of('!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=');
    private static final Set<Character> OTHER_RESERVED = Set.of('-', '.', '_', '~');
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private UriUtils() {
    }

    public static String encodePathComponent(String source) {
        if (source == null || source.isEmpty()) {
            return source;
        }
        byte[] bytes = source.getBytes(UTF_8);
        boolean original = true;
        int length = bytes.length;
        int i = 0;
        while (true) {
            if (i >= length) {
                break;
            } else if (!isAllowed(bytes[i])) {
                original = false;
                break;
            } else {
                i++;
            }
        }
        if (original) {
            return source;
        }
        ByteArrayOutputStream encodedPathByteStream = new ByteArrayOutputStream(bytes.length);
        for (byte b : bytes) {
            if (isAllowed(b)) {
                encodedPathByteStream.write(b);
            } else {
                encodedPathByteStream.write(37);
                char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 15, 16));
                char hex2 = Character.toUpperCase(Character.forDigit(b & 15, 16));
                encodedPathByteStream.write(hex1);
                encodedPathByteStream.write(hex2);
            }
        }
        return encodedPathByteStream.toString(UTF_8);
    }

    private static boolean isAllowed(int c) {
        return isPchar(c) || 47 == c;
    }

    private static boolean isPchar(int c) {
        return isUnreserved(c) || isSubDelimiter(c) || 58 == c || 64 == c;
    }

    private static boolean isUnreserved(int c) {
        return Character.isAlphabetic(c) || Character.isDigit(c) || OTHER_RESERVED.contains((char) c);
    }

    private static boolean isSubDelimiter(int c) {
        return SUB_DELIMITERS.contains((char) c);
    }
}
