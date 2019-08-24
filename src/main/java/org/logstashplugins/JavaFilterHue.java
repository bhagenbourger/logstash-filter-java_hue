package org.logstashplugins;

import co.elastic.logstash.api.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.logstash.api.PluginConfigSpec.stringSetting;

// class name must match plugin name
@LogstashPlugin(name = "java_filter_hue")
public class JavaFilterHue implements Filter {

    static final String HUE_SOURCE_FIELD_METADATA = "HUE_SOURCE_FIELD_METADATA";
    static final String LIGHTS = "lights";
    static final String SENSORS = "sensors";
    private static final String TYPE_FIELD = "type";
    private static final String TEMPERATURE_TYPE = "ZLLTemperature";
    private static final PluginConfigSpec<String> LIGHTS_SOURCE_CONFIG = stringSetting(LIGHTS, LIGHTS);
    private static final PluginConfigSpec<String> SENSORS_SOURCE_CONFIG = stringSetting(SENSORS, SENSORS);
    private static final Predicate<? super Event> LIGHTS_PREDICATE = e -> true;
    // Keep only temperature sensors
    private static final Predicate<? super Event> SENSORS_PREDICATE = e -> TEMPERATURE_TYPE.equals(e.getField(TYPE_FIELD));

    private String id;
    private String lightsSourceField;
    private String sensorsSourceField;

    public JavaFilterHue(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.lightsSourceField = config.get(LIGHTS_SOURCE_CONFIG);
        this.sensorsSourceField = config.get(SENSORS_SOURCE_CONFIG);
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {

        return events.stream()
                .flatMap(e -> generateAllEvents(e, matchListener))
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(LIGHTS_SOURCE_CONFIG, SENSORS_SOURCE_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }

    private Stream<Event> generateAllEvents(Event event, FilterMatchListener matchListener) {
        return Stream.concat(
                generateEvents(event, matchListener, lightsSourceField, LIGHTS_PREDICATE).stream(),
                generateEvents(event, matchListener, sensorsSourceField, SENSORS_PREDICATE).stream()
        );
    }

    private Collection<Event> generateEvents(
            Event event,
            FilterMatchListener matchListener,
            String sourceField,
            Predicate<? super Event> predicate
    ) {
        return getMapData(event, sourceField).values().stream()
                .map(d -> generateEvent(event, d, sourceField))
                .filter(predicate)
                .peek(e -> matchListener.filterMatched(event))
                .collect(Collectors.toSet());
    }

    private static Event generateEvent(Event originalEvent, Map<String, Object> data, String sourceField) {
        // Keep original timestamp
        final org.logstash.Event event = new org.logstash.Event(data);
        event.setEventTimestamp(originalEvent.getEventTimestamp());

        // Keep original metadata and add HUE_SOURCE_FIELD_METADATA metadata
        final Map<String, Object> metadata = originalEvent.getMetadata();
        metadata.put(HUE_SOURCE_FIELD_METADATA, sourceField);
        event.setField(org.logstash.Event.METADATA, metadata);

        return event;
    }

    private static Map<String, Map<String, Object>> getMapData(Event event, String sourceFiled) {
        return Optional
                .ofNullable((Map<String, Map<String, Object>>) event.getField(sourceFiled))
                .orElse(Collections.emptyMap());
    }
}
