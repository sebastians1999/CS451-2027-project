package cs451;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class Main {

    // Submitted defaults; env vars WINDOW / TIMEOUT_MS override them for local tuning only.
    private static final int DEFAULT_WINDOW = 1024;
    private static final int DEFAULT_TIMEOUT_MS = 100;

    private static volatile PerfectLink pl;
    private static volatile OutputLog log;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        if (pl != null) {
            pl.stop();
        }

        //write/flush output file if necessary
        System.out.println("Writing output.");
        if (log != null) {
            log.close();
        }
    }

    private static void initSignalHandlers() { // triggered during SIGINT or SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread() {  // tells the JVM to run the given thread during an orderly shutdown 
            @Override //overwrites the run mehtod
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers(); //registers shutdown hook

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        int m;
        int receiverId;
        try {
            int[] config = readPerfectLinksConfig(parser.config());
            m = config[0];
            receiverId = config[1];
            LinkConfig linkConfig = new LinkConfig(
                    envOrDefault("WINDOW", DEFAULT_WINDOW),
                    envOrDefault("TIMEOUT_MS", DEFAULT_TIMEOUT_MS));
            System.out.println("Link config: " + linkConfig + "\n");
            log = new OutputLog(parser.output());
            pl = new PerfectLink(parser.myId(), parser.hosts(), linkConfig, (src, seq, payload) -> log.delivered(src, seq));
        } catch (IOException | RuntimeException e) {
            System.err.println("Initialization failed: " + e);
            System.exit(1);
            return;
        }

        System.out.println("Broadcasting and delivering messages...\n");

        if (parser.myId() != receiverId) {
            int seq = 0;
            while (seq < m) {
                seq++;
                log.sent(seq);
                pl.send(receiverId, seq, seq);
            }
            // A full batch is sent inside send. This sends a shorter tail immediately.
            pl.flush(receiverId);
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    private static int envOrDefault(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null ? defaultValue : Integer.parseInt(value.trim());
    }

    // Remember: config line: "m i" = messages per sender, receiver id
    private static int[] readPerfectLinksConfig(String path) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String[] parts = reader.readLine().trim().split(" ");
            return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]) };
        }
    }
}
