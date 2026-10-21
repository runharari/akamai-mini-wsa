/*
 * Mini WSA data generator: realistic security events plus "attack waves".
 * JDK only, runs as a single-file source program (Java 17+).
 *
 *   java tools/EventGenerator.java --count 10000 --post http://localhost:8080
 *   java tools/EventGenerator.java --count 2000 --seed 42 --out events.json
 *   curl -H 'Content-Type: application/json' --data @events.json http://localhost:8080/v1/events/ingest
 *
 * Options: --count N (10000)  --post BASE_URL  --out FILE  --batch N (500, max 5000)  --seed N
 * At least one of --post / --out is required.
 */

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class EventGenerator {

    // ---------- configuration ----------

    record Options(int count, String postUrl, String outFile, int batch, long seed) {}

    static final int MAX_BATCH = 5000;
    static final double WAVE_SHARE = 0.30;       // ~30% of all events belong to attack waves
    static final int WINDOW_HOURS = 6;           // timestamps spread over the last 6 hours
    static final int NORMAL_IP_COUNT = 200;
    static final int ATTACKER_IP_COUNT = 20;

    // ---------- domain data ----------

    record Geo(String country, String city) {}
    record Client(String ip, Geo geo) {}
    record Site(int configId, String hostname, String policyId) {}
    record Rule(String id, String name, String message) {}

    record Event(String eventId, Instant timestamp, Site site, Client client, String path, String method,
                 int statusCode, String userAgent, String category, String severity, String action,
                 int requestSize, int responseSize) {}

    static final List<Site> SITES = List.of(
            new Site(14227, "www.example.com", "pol_web1"),
            new Site(14228, "api.example.com", "pol_api1"),
            new Site(20001, "shop.example.org", "pol_shop1"),
            new Site(20002, "admin.example.org", "pol_admin1"));

    static final List<Geo> GEOS = List.of(
            new Geo("US", "New York"), new Geo("US", "Dallas"), new Geo("DE", "Frankfurt"),
            new Geo("GB", "London"), new Geo("FR", "Paris"), new Geo("IL", "Tel Aviv"),
            new Geo("IN", "Mumbai"), new Geo("BR", "Sao Paulo"), new Geo("JP", "Tokyo"),
            new Geo("AU", "Sydney"), new Geo("CA", "Toronto"), new Geo("NL", "Amsterdam"),
            new Geo("CN", "Beijing"), new Geo("SG", "Singapore"), new Geo("ZA", "Johannesburg"));

    // Fixed category -> rule table (the API expects a consistent pairing).
    static final String[] CATEGORIES = {"INJECTION", "XSS", "PROTOCOL_VIOLATION", "DATA_LEAKAGE", "BOT", "DOS", "RATE_LIMIT"};
    static final Rule[] RULES = {
            new Rule("950001", "SQL_INJECTION", "SQL Injection Attack Detected"),
            new Rule("941100", "XSS_ATTACK", "Cross-Site Scripting Attack Detected"),
            new Rule("920100", "INVALID_HTTP_REQUEST", "Invalid HTTP Request Line"),
            new Rule("951100", "SQL_ERROR_LEAKAGE", "SQL Error Message Leakage"),
            new Rule("990100", "BAD_BOT", "Known Malicious Bot"),
            new Rule("970100", "HTTP_FLOOD", "HTTP Flood Detected"),
            new Rule("980100", "RATE_LIMIT_EXCEEDED", "Rate Limit Exceeded")};
    // Weights for normal traffic, same order as CATEGORIES.
    static final int[] CATEGORY_WEIGHTS = {20, 20, 15, 8, 15, 5, 17};

    static final String[] SEVERITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW"};
    // Severity weights per category, same order as CATEGORIES and SEVERITIES.
    static final int[][] SEVERITY_WEIGHTS = {
            {50, 35, 10, 5},    // INJECTION
            {15, 40, 35, 10},   // XSS
            {0, 10, 40, 50},    // PROTOCOL_VIOLATION
            {20, 40, 30, 10},   // DATA_LEAKAGE
            {0, 10, 40, 50},    // BOT
            {30, 40, 20, 10},   // DOS
            {0, 5, 35, 60}};    // RATE_LIMIT

    static final String[] ACTIONS = {"DENY", "ALERT", "MONITOR"};
    static final int[] ACTION_WEIGHTS = {50, 30, 20};
    static final int[] WAVE_ACTION_WEIGHTS = {85, 15, 0};

    static final String[] NORMAL_PATHS = {
            "/", "/search", "/products/{id}", "/products/{id}", "/static/app.js", "/static/style.css",
            "/api/v1/orders", "/api/v1/users/{id}", "/api/v1/login", "/api/v1/cart", "/checkout",
            "/admin", "/admin/users", "/wp-login.php", "/contact", "/api/v1/products?page={id}"};
    static final String[] WAVE_PATHS = {"/api/v1/login", "/api/v1/login", "/admin", "/wp-login.php"};
    static final String[] WAVE_CATEGORIES = {"INJECTION", "BOT", "DOS"};

    static final String[] BROWSER_AGENTS = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148"};
    static final String[] TOOL_AGENTS = {"curl/8.4.0", "python-requests/2.31.0", "Go-http-client/1.1"};
    static final String[] ATTACK_AGENTS = {"sqlmap/1.7.2#stable (https://sqlmap.org)", "Nikto/2.5.0", "masscan/1.3", "python-requests/2.31.0"};
    static final String[] BOT_AGENTS = {"Mozilla/5.0 (compatible; AhrefsBot/7.0)", "Mozilla/5.0 (compatible; SemrushBot/7~bl)", "Scrapy/2.11.0"};

    // ---------- generator state ----------

    final Random random;
    final Options options;
    final Instant now = Instant.now();
    final String runId;
    final List<Client> normalClients = new ArrayList<>();
    final List<Client> attackers = new ArrayList<>();
    int sequence = 0;

    EventGenerator(Options options) {
        this.options = options;
        this.random = new Random(options.seed());
        // The run id is deliberately NOT seeded, so two runs with the same seed never reuse event ids.
        this.runId = Long.toString(new Random().nextInt(36 * 36 * 36 * 36), 36);
        buildClientPools();
    }

    // ---------- main ----------

    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);
        if (options == null) {
            System.exit(2);
        }
        EventGenerator generator = new EventGenerator(options);
        List<Event> events = generator.generateAll();
        List<String> json = events.stream().map(EventGenerator::toJson).toList();

        if (options.outFile() != null) {
            writeFile(options, json);
        }
        if (options.postUrl() != null) {
            boolean ok = post(options, json);
            if (!ok) {
                System.exit(1);
            }
        }
    }

    // ---------- argument parsing ----------

    static final String USAGE = """
            Usage: java tools/EventGenerator.java [options]
              --count N    number of events (default 10000)
              --post URL   base URL of the ingest service, e.g. http://localhost:8080
              --out FILE   write all events as one JSON array to FILE
              --batch N    events per POST (default 500, max 5000)
              --seed N     random seed for a reproducible run (default: random)
            At least one of --post / --out is required.""";

    static Options parseArgs(String[] args) {
        int count = 10000;
        String postUrl = null;
        String outFile = null;
        int batch = 500;
        long seed = new Random().nextLong();
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--help", "-h" -> { System.out.println(USAGE); return null; }
                    case "--count" -> count = Integer.parseInt(args[++i]);
                    case "--post" -> postUrl = args[++i];
                    case "--out" -> outFile = args[++i];
                    case "--batch" -> batch = Integer.parseInt(args[++i]);
                    case "--seed" -> seed = Long.parseLong(args[++i]);
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }
            if (count < 1) throw new IllegalArgumentException("--count must be at least 1");
            if (batch < 1 || batch > MAX_BATCH) throw new IllegalArgumentException("--batch must be 1.." + MAX_BATCH);
            if (postUrl == null && outFile == null) throw new IllegalArgumentException("Give --post and/or --out");
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Missing value for the last option\n" + USAGE);
            return null;
        } catch (IllegalArgumentException e) { // NumberFormatException is a subclass
            System.err.println("Error: " + e.getMessage() + "\n" + USAGE);
            return null;
        }
        return new Options(count, postUrl, outFile, batch, seed);
    }

    // ---------- event generation ----------

    List<Event> generateAll() {
        int waveTarget = (int) (options.count() * WAVE_SHARE);
        List<Event> events = new ArrayList<>(options.count());
        generateWaves(events, waveTarget);
        while (events.size() < options.count()) {
            events.add(normalEvent());
        }
        // Send in timestamp order: the pipeline's repeat-offender window keeps only the last
        // few events per IP, so shuffled timestamps would under-count attack waves.
        events.sort(Comparator.comparing(Event::timestamp));
        return events;
    }

    void generateWaves(List<Event> events, int waveTarget) {
        while (events.size() < waveTarget) {
            int size = Math.min(10 + random.nextInt(51), waveTarget - events.size());
            generateWave(events, size);
        }
    }

    // One wave: one attacker IP hits one path, one event every 2-20 seconds.
    void generateWave(List<Event> events, int size) {
        Client attacker = attackers.get(random.nextInt(attackers.size()));
        Site site = SITES.get(random.nextInt(SITES.size()));
        String path = WAVE_PATHS[random.nextInt(WAVE_PATHS.length)];
        String category = WAVE_CATEGORIES[random.nextInt(WAVE_CATEGORIES.length)];
        String method = path.equals("/admin") ? "GET" : "POST";
        // Leave room before "now" so the whole wave (at most ~20 min) is in the past.
        long latestStartSec = WINDOW_HOURS * 3600L - 25 * 60;
        Instant time = now.minusSeconds(25 * 60 + (long) (random.nextDouble() * latestStartSec));
        for (int i = 0; i < size; i++) {
            events.add(buildEvent(time, site, attacker, path, method, category, WAVE_ACTION_WEIGHTS, true));
            time = time.plusSeconds(2 + random.nextInt(19));
        }
    }

    Event normalEvent() {
        Instant time = now.minusSeconds(10 + (long) (random.nextDouble() * (WINDOW_HOURS * 3600L - 10)));
        Site site = SITES.get(random.nextInt(SITES.size()));
        Client client = normalClients.get(random.nextInt(normalClients.size()));
        String path = NORMAL_PATHS[random.nextInt(NORMAL_PATHS.length)].replace("{id}", String.valueOf(1 + random.nextInt(5000)));
        String method = random.nextInt(100) < 65 ? "GET" : (random.nextInt(100) < 85 ? "POST" : "PUT");
        String category = CATEGORIES[weighted(CATEGORY_WEIGHTS)];
        return buildEvent(time, site, client, path, method, category, ACTION_WEIGHTS, false);
    }

    Event buildEvent(Instant time, Site site, Client client, String path, String method,
                     String category, int[] actionWeights, boolean attack) {
        int categoryIndex = indexOf(CATEGORIES, category);
        String severity = SEVERITIES[weighted(SEVERITY_WEIGHTS[categoryIndex])];
        String action = ACTIONS[weighted(actionWeights)];
        String eventId = "evt-" + runId + "-" + String.format("%06d", ++sequence);
        return new Event(eventId, time.truncatedTo(ChronoUnit.SECONDS), site, client, path, method,
                statusFor(action), userAgentFor(category, attack), category, severity, action,
                200 + random.nextInt(4000), 100 + random.nextInt(50000));
    }

    int statusFor(String action) {
        if (action.equals("DENY")) return 403;
        int roll = random.nextInt(100);
        if (roll < 5) return 404;
        if (roll < 7) return 500;
        return 200;
    }

    String userAgentFor(String category, boolean attack) {
        if (attack && category.equals("BOT")) return pick(BOT_AGENTS);
        if (attack) return pick(ATTACK_AGENTS);
        int roll = random.nextInt(100);
        if (roll < 75) return pick(BROWSER_AGENTS);
        if (roll < 90) return pick(TOOL_AGENTS);
        return pick(BOT_AGENTS);
    }

    // ---------- client pools ----------

    void buildClientPools() {
        String[] prefixes = {"192.0.2.", "198.51.100.", "203.0.113.", "81.12.40.", "45.33.7.", "104.18.22."};
        while (normalClients.size() < NORMAL_IP_COUNT) {
            String ip = prefixes[random.nextInt(prefixes.length)] + (1 + random.nextInt(254));
            addClient(normalClients, ip, GEOS);
        }
        while (attackers.size() < ATTACKER_IP_COUNT) {
            String ip = "185." + (100 + random.nextInt(100)) + "." + random.nextInt(256) + "." + (1 + random.nextInt(254));
            addClient(attackers, ip, GEOS);
        }
    }

    // Each IP gets its country/city once, so the same IP is always in the same place.
    void addClient(List<Client> pool, String ip, List<Geo> geos) {
        boolean known = pool.stream().anyMatch(c -> c.ip().equals(ip));
        if (!known) {
            pool.add(new Client(ip, geos.get(random.nextInt(geos.size()))));
        }
    }

    // ---------- small random helpers ----------

    String pick(String[] values) {
        return values[random.nextInt(values.length)];
    }

    int weighted(int[] weights) {
        int total = 0;
        for (int w : weights) total += w;
        int roll = random.nextInt(total);
        for (int i = 0; i < weights.length; i++) {
            roll -= weights[i];
            if (roll < 0) return i;
        }
        return weights.length - 1;
    }

    static int indexOf(String[] values, String value) {
        for (int i = 0; i < values.length; i++) {
            if (values[i].equals(value)) return i;
        }
        throw new IllegalArgumentException(value);
    }

    // ---------- JSON ----------

    static String toJson(Event e) {
        Rule rule = RULES[indexOf(CATEGORIES, e.category())];
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        field(sb, "eventId", e.eventId()).append(',');
        field(sb, "timestamp", e.timestamp().toString()).append(',');
        sb.append("\"configId\":").append(e.site().configId()).append(',');
        field(sb, "policyId", e.site().policyId()).append(',');
        field(sb, "clientIp", e.client().ip()).append(',');
        field(sb, "hostname", e.site().hostname()).append(',');
        field(sb, "path", e.path()).append(',');
        field(sb, "method", e.method()).append(',');
        sb.append("\"statusCode\":").append(e.statusCode()).append(',');
        field(sb, "userAgent", e.userAgent()).append(',');
        sb.append("\"rule\":{");
        field(sb, "id", rule.id()).append(',');
        field(sb, "name", rule.name()).append(',');
        field(sb, "message", rule.message()).append(',');
        field(sb, "severity", e.severity()).append(',');
        field(sb, "category", e.category()).append("},");
        field(sb, "action", e.action()).append(',');
        sb.append("\"geoLocation\":{");
        field(sb, "country", e.client().geo().country()).append(',');
        field(sb, "city", e.client().geo().city()).append("},");
        sb.append("\"requestSize\":").append(e.requestSize()).append(',');
        sb.append("\"responseSize\":").append(e.responseSize());
        return sb.append('}').toString();
    }

    static StringBuilder field(StringBuilder sb, String name, String value) {
        return sb.append('"').append(name).append("\":\"").append(escape(value)).append('"');
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    static String jsonArray(List<String> items) {
        return "[" + String.join(",", items) + "]";
    }

    // ---------- output ----------

    static void writeFile(Options options, List<String> json) throws IOException {
        Files.writeString(Path.of(options.outFile()), jsonArray(json));
        System.out.println("Wrote " + json.size() + " events to " + options.outFile());
        if (json.size() > MAX_BATCH) {
            System.out.println("Note: the API accepts at most " + MAX_BATCH
                    + " events per request, so this file is too big for a single curl; use --post instead.");
        }
    }

    static boolean post(Options options, List<String> json) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI uri = URI.create(options.postUrl().replaceAll("/+$", "") + "/v1/events/ingest");
        long start = System.nanoTime();
        int sent = 0;
        int nextReport = 2000;
        for (int from = 0; from < json.size(); from += options.batch()) {
            List<String> batch = json.subList(from, Math.min(from + options.batch(), json.size()));
            if (!sendBatch(client, uri, batch)) {
                return false;
            }
            sent += batch.size();
            if (sent >= nextReport) {
                System.out.println("Progress: " + sent + "/" + json.size());
                nextReport += 2000;
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("Sent %d events in %.1fs (%.0f events/s)%n", sent, seconds, sent / seconds);
        return true;
    }

    // Returns false if the server rejected the batch (400) or stayed unavailable.
    static boolean sendBatch(HttpClient client, URI uri, List<String> batch) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(jsonArray(batch)))
                .build();
        for (int attempt = 1; attempt <= 5; attempt++) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return true;
            }
            if (status == 503) {
                long wait = response.headers().firstValueAsLong("Retry-After").orElse(5);
                System.out.println("503 from server, retrying in " + wait + "s (attempt " + attempt + "/5)");
                Thread.sleep(wait * 1000);
                continue;
            }
            System.err.println("HTTP " + status + ": " + response.body());
            return false;
        }
        System.err.println("Server still unavailable after 5 attempts, giving up");
        return false;
    }
}
