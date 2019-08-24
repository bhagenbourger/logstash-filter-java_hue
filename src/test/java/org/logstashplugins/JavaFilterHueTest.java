package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JavaFilterHueTest {

    private static final String DATA_FILE = "data.json";

    @Test
    public void shouldSplitEachLightInEvent() throws IOException {
        final String sourceField = "lights";
        final Configuration config = new ConfigurationImpl(Collections.singletonMap("source", sourceField));
        final Context context = new ContextImpl(null, null);
        final JavaFilterHue filter = new JavaFilterHue("test-id", config, context);
        final Event event = generateEvent();
        final TestMatchListener matchListener = new TestMatchListener();

        final Collection<Event> results = filter.filter(Collections.singleton(event), matchListener);

        // Lights are split into several events
        Assert.assertEquals(5, results.size());
        Assert.assertEquals(5, matchListener.getMatchCount());

        // Original timestamp is kept
        Assert.assertTrue(results.stream().allMatch(e -> e.getEventTimestamp().equals(event.getEventTimestamp())));

        // Original metadata are kept
        Assert.assertTrue(
                results.stream().allMatch(
                        e -> event.getMetadata().entrySet().stream().allMatch(
                                m -> e.getMetadata().get(m.getKey()).equals(m.getValue())))
        );
    }

    private Event generateEvent() throws IOException {
        final org.logstash.Event event = new org.logstash.Event(jsonToMap(getJsonData(getDataFile())));
        event.setField(org.logstash.Event.METADATA, generateMetadata());
        return event;
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