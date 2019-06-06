package com.jiaxy.monitor.demo.opencensus;

import io.opencensus.common.Scope;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import io.prometheus.client.exporter.HTTPServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Repl {
    // The latency in milliseconds
    private static final MeasureDouble M_LATENCY_MS = MeasureDouble.create("repl/latency_2", "The latency in milliseconds per REPL loop",
            "ms");

    // Counts/groups the lengths of lines read in.
    private static final MeasureLong M_LINE_LENGTHS = MeasureLong.create("repl/line_lengths_2", "The distribution of line lengths", "By");

    // The tag "method"
    private static final TagKey KEY_METHOD = TagKey.create("method");
    // The tag "status"
    private static final TagKey KEY_STATUS = TagKey.create("status");
    // The tag "error"
    private static final TagKey KEY_ERROR  = TagKey.create("error");

    private static final Tagger        tagger        = Tags.getTagger();
    private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

    public static void main(String... args) {
        // Step 1. Enable OpenCensus Metrics.
        try {
            setupOpenCensusAndPrometheusExporter();
        } catch (IOException e) {
            System.err.println("Failed to create and register OpenCensus Prometheus Stats exporter " + e);
            return;
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            long startTimeNs = System.nanoTime();

            try {
                readEvaluateProcessLine(stdin);
                TagKey[] tagKeys = {KEY_METHOD, KEY_STATUS};
                String[] tagValues = {"repl", "OK"};
                recordTaggedStat(tagKeys, tagValues, M_LATENCY_MS,
                        sinceInMilliseconds(startTimeNs));
            } catch (IOException e) {
                System.err.println("EOF bye " + e);
                return;
            } catch (Exception e) {
                TagKey[] tagKeys = {KEY_METHOD, KEY_STATUS, KEY_ERROR};
                String[] tagValues = {"repl", "ERROR", e.getMessage()};
                recordTaggedStat(tagKeys, tagValues, M_LATENCY_MS,
                        sinceInMilliseconds(startTimeNs));
                return;
            }
        }
    }

    private static void recordStat(MeasureLong ml, Long n) {
        TagContext tctx = tagger.emptyBuilder().build();
        try (Scope ss = tagger.withTagContext(tctx)) {
            statsRecorder.newMeasureMap().put(ml, n).record();
        }
    }

    private static void recordTaggedStat(TagKey key, String value, MeasureLong ml, Long n) {
        TagContext tctx = tagger.emptyBuilder().put(key, TagValue.create(value)).build();
        try (Scope ss = tagger.withTagContext(tctx)) {
            statsRecorder.newMeasureMap().put(ml, n).record();
        }
    }

    private static void recordTaggedStat(TagKey key, String value, MeasureDouble md, Double d) {
        TagContext tctx = tagger.emptyBuilder().put(key, TagValue.create(value)).build();
        try (Scope ss = tagger.withTagContext(tctx)) {
            statsRecorder.newMeasureMap().put(md, d).record();
        }
    }

    private static void recordTaggedStat(TagKey[] keys, String[] values, MeasureDouble md, Double d) {
        TagContextBuilder builder = tagger.emptyBuilder();
        for (int i = 0; i < keys.length; i++) {
            builder.put(keys[i], TagValue.create(values[i]));
        }
        TagContext tctx = builder.build();

        try (Scope ss = tagger.withTagContext(tctx)) {
            statsRecorder.newMeasureMap().put(md, d).record();
        }
    }

    private static String processLine(String line) {
        long startTimeNs = System.nanoTime();

        try {
            return line.toUpperCase();
        } finally {
            TagKey[] tagKeys = {KEY_METHOD, KEY_STATUS};
            String[] tagValues = {"repl", "OK"};
            recordTaggedStat(tagKeys, tagValues, M_LATENCY_MS, sinceInMilliseconds(startTimeNs));
        }
    }

    private static double sinceInMilliseconds(long startTimeNs) {
        return (new Double(System.nanoTime() - startTimeNs)) / 1e6;
    }

    private static void readEvaluateProcessLine(BufferedReader in) throws IOException {
        System.out.print("> ");
        System.out.flush();

        String line = in.readLine();
        String processed = processLine(line);
        System.out.println("< " + processed + "\n");
        if (line != null && line.length() > 0) {
            recordStat(M_LINE_LENGTHS, new Long(line.length()));
        }
    }

    private static void registerAllViews() {
        // Defining the distribution aggregations
        Aggregation latencyDistribution = Distribution.create(BucketBoundaries.create(
                Arrays.asList(
                        // [>=0ms, >=25ms, >=50ms, >=75ms, >=100ms, >=200ms, >=400ms, >=600ms, >=800ms, >=1s, >=2s, >=4s, >=6s]
                        0.0, 25.0, 50.0, 75.0, 100.0, 200.0, 400.0, 600.0, 800.0, 1000.0, 2000.0, 4000.0, 6000.0)
        ));

        Aggregation lengthsDistribution = Distribution.create(BucketBoundaries.create(
                Arrays.asList(
                        // [>=0B, >=5B, >=10B, >=20B, >=40B, >=60B, >=80B, >=100B, >=200B, >=400B, >=600B, >=800B, >=1000B]
                        0.0, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0, 200.0, 400.0, 600.0, 800.0, 1000.0)
        ));

        // Define the count aggregation
        Aggregation countAggregation = Aggregation.Count.create();

        // So tagKeys
        List<TagKey> noKeys = new ArrayList<TagKey>();

        // Define the views
        View[] views = new View[] {
                View.create(Name.create("ocjavametrics/latency_demo"), "The distribution of latencies", M_LATENCY_MS, latencyDistribution,
                        Collections.unmodifiableList(Arrays.asList(KEY_METHOD, KEY_STATUS, KEY_ERROR))),
                View.create(Name.create("ocjavametrics/lines_in_demo"), "The number of lines read in from standard input", M_LINE_LENGTHS,
                        countAggregation, noKeys),
                View.create(Name.create("ocjavametrics/line_lengths_demo"), "The distribution of line lengths", M_LINE_LENGTHS,
                        lengthsDistribution, noKeys)
        };

        // Create the view manager
        ViewManager vmgr = Stats.getViewManager();

        // Then finally register the views
        for (View view : views) { vmgr.registerView(view); }
    }

    private static void setupOpenCensusAndPrometheusExporter() throws IOException {
        // Firstly register the views
        registerAllViews();

        // Create and register the Prometheus exporter
        PrometheusStatsCollector.createAndRegister();

        // Run the server as a daemon on address "localhost:8889"
        HTTPServer server = new HTTPServer("localhost", 8889, true);
    }
}

