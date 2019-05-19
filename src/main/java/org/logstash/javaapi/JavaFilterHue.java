package org.logstash.javaapi;

import co.elastic.logstash.api.*;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

// class name must match plugin name
@LogstashPlugin(name = "java_filter_hue")
public class JavaFilterHue implements Filter {

    private static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("lights", "lights");

    private String id;
    private String sourceField;

    public JavaFilterHue(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {

        return events.stream()
                .flatMap(e -> generateLightEvents(
                        e.getEventTimestamp(),
                        (Map<String, Map<String, Object>>) e.getField(sourceField),
                        matchListener
                ).stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Collections.singletonList(SOURCE_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }

    private Collection<Event> generateLightEvents(
            Instant originalTimestamp,
            Map<String, Map<String, Object>> lights,
            FilterMatchListener matchListener
    ) {
        return lights.values().stream()
                .map(l -> generateLightEvent(originalTimestamp, l, matchListener))
                .collect(Collectors.toSet());
    }

    private Event generateLightEvent(
            Instant originalTimestamp,
            Map<String, Object> light,
            FilterMatchListener matchListener
    ) {
        final org.logstash.Event lightEvent = new org.logstash.Event(light);
        lightEvent.setEventTimestamp(originalTimestamp);
        matchListener.filterMatched(lightEvent);
        return lightEvent;
    }
}
