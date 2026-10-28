import org.apache.nifi.stateless.bootstrap.StatelessBootstrap;
import org.apache.nifi.stateless.config.PropertiesFileEngineConfigurationParser;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Runs r2dfly's one-time copy on NiFi's Stateless engine and exits once it is done. NiFi's own
 * nifi-stateless.sh either stops after the scanner's first page or never stops, so this triggers the
 * flow until the scanner has completed every partition and nothing is queued.
 *
 * <p>java -cp "STATELESS/lib/*:STATELESS/conf" HeadlessCopy.java ENGINE_PROPERTIES FLOW_PROPERTIES PARTITIONS
 *
 * <p>Exit status: 0 done, 1 a trigger failed, 2 the flow is invalid, 3 no progress for five minutes.
 */
public class HeadlessCopy {

    private static final long STALL_MS = 300_000;
    private static final Pattern COUNTERS = Pattern.compile(".*(Scan Partitions Complete|Keys Scanned|Keys Written|Write Errors|Dead-Letter Records Pending)$");

    public static void main(String[] args) throws Exception {
        int partitions = Integer.parseInt(args[2]);
        var engine = new PropertiesFileEngineConfigurationParser().parseEngineConfiguration(new File(args[0]));
        var bootstrap = StatelessBootstrap.bootstrap(engine);
        StatelessDataflow flow = bootstrap.createDataflow(bootstrap.parseDataflowDefinition(new File(args[1]), List.of()));
        flow.initialize(() -> true);
        var validation = flow.performValidation();
        if (!validation.isValid()) {
            System.err.println("headless: the flow is invalid: " + validation);
            System.exit(2);
        }
        long started = System.currentTimeMillis();
        long lastProgressAt = started;
        long lastScanned = -1;
        int exit = 0;
        try {
            while (true) {
                TriggerResult result = flow.trigger().getResult();
                if (!result.isSuccessful()) {
                    Throwable cause = result.getFailureCause().orElse(new IllegalStateException("the trigger was canceled"));
                    System.err.println("headless: a trigger failed: " + cause);
                    result.abort(cause);
                    exit = 1;
                    break;
                }
                result.acknowledge();
                Map<String, Long> counters = flow.getCounters(COUNTERS);
                boolean queued = flow.isFlowFileQueued();
                if (counter(counters, "Scan Partitions Complete") >= partitions && !queued) {
                    break;
                }
                long scanned = counter(counters, "Keys Scanned");
                long now = System.currentTimeMillis();
                if (scanned != lastScanned || queued) {
                    lastScanned = scanned;
                    lastProgressAt = now;
                } else if (now - lastProgressAt > STALL_MS) {
                    System.err.println("headless: no progress for " + STALL_MS / 1000 + " s; stopping");
                    exit = 3;
                    break;
                }
            }
            Map<String, Long> counters = flow.getCounters(COUNTERS);
            System.out.printf("headless: keys scanned %d, keys written %d, write errors %d, dead-letter records %d, %d s%n",
                    counter(counters, "Keys Scanned"), counter(counters, "Keys Written"), counter(counters, "Write Errors"),
                    counter(counters, "Dead-Letter Records Pending"), (System.currentTimeMillis() - started) / 1000);
        } finally {
            flow.shutdown();
        }
        System.exit(exit);
    }

    /** Counters are listed per component and again as an "All ..." total; the largest is the total. */
    private static long counter(Map<String, Long> counters, String name) {
        return counters.entrySet().stream().filter(e -> e.getKey().endsWith(name)).mapToLong(Map.Entry::getValue).max().orElse(0);
    }
}
