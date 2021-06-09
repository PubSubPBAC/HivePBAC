package de.karlw.pbac;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.general.Qos;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.publish.Publish;
import com.hivemq.extension.sdk.api.services.publish.PublishService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PbacMetrics {

    private static final @NotNull Logger log = LoggerFactory.getLogger(PbacMetrics.class);

    public PbacMetrics() {
        registerMetrics();
    }

    private void registerMetrics() {
        final MetricRegistry metricRegistry = Services.metricRegistry();

        // create the new metric, that is increased when a client isn't successfully authenticated
        final Counter reservationsCounter = metricRegistry.counter("de.karlw.pbac.reservations");
        final Counter allowedByApCpunter = metricRegistry.counter("de.karlw.pbac.allowed_no_ap");
        final Counter allowedWithoutApCounter = metricRegistry.counter("de.karlw.pbac.allowed_ap");
        final Counter forbiddenCounter = metricRegistry.counter("de.karlw.pbac.forbidden");

        Services.extensionExecutorService().scheduleAtFixedRate(
                this::publishMetrics,
                1,
                1,
                TimeUnit.SECONDS
        );

        log.debug("metrics registered");
    }


    private void publishMetrics() {
//        log.debug("loggedi log");
        Map<String, String> metrics = new HashMap<>();

        final MetricRegistry mr = Services.metricRegistry();
        final PublishService publishService = Services.publishService();
//        final SubscriptionStore store = Services.subscriptionStore();

        metrics.put("reservations", String.valueOf(mr.counter("de.karlw.pbac.reservations").getCount()));
        metrics.put("allowed_ap", String.valueOf(mr.counter("de.karlw.pbac.allowed_ap").getCount()));
        metrics.put("allowed_no_ap", String.valueOf(mr.counter("de.karlw.pbac.allowed_no_ap").getCount()));
        metrics.put("forbidden", String.valueOf(mr.counter("de.karlw.pbac.forbidden").getCount()));
        long subs = Services.metricRegistry().getCounters().get("com.hivemq.subscriptions.overall.current").getCount();
        long dropped = Services.metricRegistry().getCounters().get("com.hivemq.messages.dropped.count").getCount();
        metrics.put("subscriptions", String.valueOf(subs));
        metrics.put("dropped", String.valueOf(dropped));
//        metrics.put("cluster_overload_level", String.valueOf(
//                mr.getCounters()
//                        .get("com.hivemq.supervision.overload.protection.level")
//                                .getCount()
//                        ));

        for (Map.Entry<String, Counter> e : mr.getCounters().entrySet()) {
            metrics.put(e.getKey(), String.valueOf(e.getValue().getCount()));
        }

        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        metrics.put("timestamp", timeStamp);

        for (Map.Entry<String, String> metric : metrics.entrySet()) {
            Publish message = Builders.publish()
                    .topic("$SYS/pbac/" + metric.getKey())
                    .qos(Qos.AT_LEAST_ONCE)
                    .payload(makePayload(metric.getValue()))
                    .build();

            Services.publishService().publish(message);
        }

        PurposeSettings.getInstance().getSettings().forEach((key, value) ->  {
            Publish message = Builders.publish()
                    .topic("$SYS/pbac/settings/" + key)
                    .qos(Qos.AT_LEAST_ONCE)
                    .payload(makePayload(String.valueOf(value)))
                    .build();

            Services.publishService().publish(message);
        });

    }

    private ByteBuffer makePayload(String message) {
        return ByteBuffer.wrap(message.getBytes());
    }


}
