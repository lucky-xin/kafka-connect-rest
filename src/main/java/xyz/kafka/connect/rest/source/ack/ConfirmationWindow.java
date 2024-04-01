package xyz.kafka.connect.rest.source.ack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.function.Function.identity;
import static xyz.kafka.connect.rest.utils.CollectionUtils.toLinkedHashMap;

/**
 * ConfirmationWindow
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class ConfirmationWindow<T> {

    private static final Logger log = LoggerFactory.getLogger(ConfirmationWindow.class);

    private final LinkedHashMap<T, Boolean> confirmedOffsets;

    public ConfirmationWindow(List<T> offsets) {
        confirmedOffsets = offsets.stream()
                .collect(toLinkedHashMap(identity(), __ -> false));
    }

    public void confirm(T offset) {
        confirmedOffsets.replace(offset, true);

        log.debug("Confirmed offset {}", offset);
    }

    public Optional<T> getLowWatermarkOffset() {
        T offset = null;
        for (Map.Entry<T, Boolean> offsetEntry : confirmedOffsets.entrySet()) {
            Boolean offsetWasConfirmed = offsetEntry.getValue();
            T sourceOffset = offsetEntry.getKey();
            if (Boolean.TRUE.equals(offsetWasConfirmed)) {
                offset = sourceOffset;
            } else {
                log.warn("Found unconfirmed offset {}. Will resume polling from previous offset. " +
                        "This might result in a number of duplicated records.", sourceOffset);
                break;
            }
        }

        return Optional.ofNullable(offset);
    }
}
