package org.logstashplugins;

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
                .map(e -> (Map<String, Object>) e.getField(sourceField))
                .flatMap(m -> m.values().stream())
                .map(m -> getEvent((Map<String, Object>) m, matchListener))
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

    private org.logstash.Event getEvent(Map<String, Object> data, FilterMatchListener matchListener) {
        final org.logstash.Event event = new org.logstash.Event(data);
        matchListener.filterMatched(event);
        return event;
    }
}
