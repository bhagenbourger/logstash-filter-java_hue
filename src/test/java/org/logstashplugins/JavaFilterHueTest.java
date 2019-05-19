package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JavaFilterHueTest {

    @Test
    public void shouldSplitEachLightInEvent() {
        final String sourceField = "lights";
        final Map<String, Object> data = new HashMap<>();
        final Map<String, Object> foo = new HashMap<>();
        final Map<String, Object> data1 = new HashMap<>();
        data1.put("capabilities", "test1");
        final Map<String, Object> data2 = new HashMap<>();
        data2.put("capabilities", "test2");
        final Map<String, Object> data3 = new HashMap<>();
        data3.put("capabilities", "test3");
        final Map<String, Object> data4 = new HashMap<>();
        data4.put("capabilities", "test4");
        final Map<String, Object> data5 = new HashMap<>();
        data5.put("capabilities", "test5");
        foo.put("1", data1);
        foo.put("2", data2);
        foo.put("3", data3);
        foo.put("4", data4);
        foo.put("5", data5);
        data.put(sourceField, foo);

        final Configuration config = new ConfigurationImpl(Collections.singletonMap("source", sourceField));
        final Context context = new ContextImpl(null);
        final JavaFilterHue filter = new JavaFilterHue("test-id", config, context);
        final Event event = new org.logstash.Event(data);
        final TestMatchListener matchListener = new TestMatchListener();

        final Collection<Event> results = filter.filter(Collections.singletonList(event), matchListener);

        Assert.assertEquals(5, results.size());
        Assert.assertEquals(5, matchListener.getMatchCount());
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