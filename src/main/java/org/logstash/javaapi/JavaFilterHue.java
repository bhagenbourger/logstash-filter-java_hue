package org.logstash.javaapi;

import co.elastic.logstash.api.*;

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
                .flatMap(e -> generateLightEvents(e, matchListener).stream())
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

    private Collection<Event> generateLightEvents(Event event, FilterMatchListener matchListener) {
        return ((Map<String, Map<String, Object>>) event.getField(sourceField)).values().stream()
                .map(l -> generateLightEvent(event, l, matchListener))
                .collect(Collectors.toSet());
    }

    private Event generateLightEvent(Event event, Map<String, Object> light, FilterMatchListener matchListener) {
        final org.logstash.Event lightEvent = new org.logstash.Event(light);
        lightEvent.setEventTimestamp(event.getEventTimestamp());
        lightEvent.setField(org.logstash.Event.METADATA, event.getMetadata());
        matchListener.filterMatched(lightEvent);
        return lightEvent;
    }
}
