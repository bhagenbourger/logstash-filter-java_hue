package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.logstashplugins.JavaFilterHue.*;

public class JavaFilterHueTest {

    private static final String DATA_FILE = "data.json";

    private Event event = generateEvent();
    private TestMatchListener matchListener;

    @Before
    public void setUp() {
        matchListener = new TestMatchListener();
    }

    @Test
    public void shouldSplitEachLightAndEachTemperatureSensorInEvent() {
        final Configuration config = new ConfigurationImpl(Collections.emptyMap());
        final Context context = new ContextImpl(null, null);
        final JavaFilterHue filter = new JavaFilterHue("test-id", config, context);

        final Collection<Event> results = filter.filter(Collections.singleton(event), matchListener);

        // Lights and temperature sensors are split into several events
        Assert.assertEquals(7, results.size());
        Assert.assertEquals(7, matchListener.getMatchCount());

        // Should keep 5 lights
        Assert.assertEquals(5, count(results, LIGHTS));

        // Should keep 2 temperature sensors
        Assert.assertEquals(2, count(results, SENSORS));

        // Original timestamp is kept
        assertOriginalTimestampIsKept(results);

        // Original metadata are kept
        assertOriginalMetadataAreKept(results);

    }

    @Test
    public void shouldNotFindLightsAndTemperatureSensorWhenConfigIsWrong() {
        final Map<String, Object> configData = new HashMap<>();
        configData.put(LIGHTS, "wrong");
        configData.put(SENSORS, "bad");
        final Configuration config = new ConfigurationImpl(configData);
        final Context context = new ContextImpl(null, null);
        final JavaFilterHue filter = new JavaFilterHue("test-id", config, context);

        final Collection<Event> results = filter.filter(Collections.singleton(event), matchListener);

        Assert.assertTrue(results.isEmpty());
    }

    private void assertOriginalTimestampIsKept(Collection<Event> results) {
        Assert.assertTrue(results.stream().allMatch(e -> e.getEventTimestamp().equals(event.getEventTimestamp())));
    }

    private void assertOriginalMetadataAreKept(Collection<Event> results) {
        final Predicate<? super Map.Entry<String, Object>> excludeHueSourceFieldMetadate = m -> !HUE_SOURCE_FIELD_METADATA.equals(m.getKey());
        Assert.assertTrue(
                results.stream().allMatch(
                        e -> event.getMetadata().entrySet().stream().filter(excludeHueSourceFieldMetadate).allMatch(
                                m -> e.getMetadata().get(m.getKey()).equals(m.getValue())))
        );
    }

    private Event generateEvent() {
        try {
            final org.logstash.Event event = new org.logstash.Event(jsonToMap(getJsonData(getDataFile())));
            event.setField(org.logstash.Event.METADATA, generateMetadata());
            return event;
        } catch (IOException e) {
            return null;
        }
    }

    private String getDataFile() {
        return getClass().getClassLoader().getResource(DATA_FILE).getFile();
    }

    private static String getJsonData(String dataFile) throws IOException {
        final FileReader fileReader = new FileReader(dataFile);
        final BufferedReader bufferedReader = new BufferedReader(fileReader);

        final StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line);
        }

        bufferedReader.close();
        fileReader.close();


        return stringBuilder.toString();
    }

    private static Map<String, Object> generateMetadata() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("id", "test");
        return metadata;
    }

    private static Map<? extends Serializable, Object> jsonToMap(String json) {
        final Gson gson = new Gson();
        return gson.fromJson(json, Map.class);
    }

    private static long count(Collection<Event> results, String hueSourceFieldMetadataValue) {
        return results.stream()
                .filter(e -> hueSourceFieldMetadataValue.equals(e.getMetadata().get(HUE_SOURCE_FIELD_METADATA).toString()))
                .count();
    }
}

class TestMatchListener implements FilterMatchListener {

    private AtomicInteger matchCount = new AtomicInteger(0);

    @Override
    public void filterMatched(Event event) {
        matchCount.incrementAndGet();
    }

    int getMatchCount() {
        return matchCount.get();
    }
}