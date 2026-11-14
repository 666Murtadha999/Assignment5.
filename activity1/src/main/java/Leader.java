import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public final class Leader implements AutoCloseable {
    private static final Pattern WORKER_ID = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)");
    private static final Pattern TASK = Pattern.compile(
            "(?i:calculate\\s+)?[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)\\s*[+*\\-]\\s*"
                    + "[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)");

    private final Object stateLock = new Object();
    private final Map<String, WorkerConnection> workers = new LinkedHashMap<>();
    private final ExecutorService readers = Executors.newCachedThreadPool(job -> {
        Thread thread = new Thread(job, "worker-connection");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ServerSocket server;
    private final int timeoutSeconds;
    private Round activeRound;
    private long nextRoundId = 1;

    private Leader(int port, int timeoutSeconds) throws IOException {
        this.server = new ServerSocket(port);
        this.timeoutSeconds = timeoutSeconds;
    }

    public static void main(String[] args) {
        if (args.length > 2) {
            System.err.println("Usage: Leader [port] [timeoutSeconds]");
            System.exit(1);
        }
        try {
            int port = args.length >= 1 ? Integer.parseInt(args[0]) : 9000;
            int timeout = args.length == 2 ? Integer.parseInt(args[1]) : 30;
            if (port < 1 || port > 65535 || timeout < 1 || timeout > 3600) {
                throw new IllegalArgumentException("Port must be 1-65535; timeout must be 1-3600 seconds.");
            }
            try (Leader leader = new Leader(port, timeout)) {
                Runtime.getRuntime().addShutdownHook(new Thread(leader::close, "leader-shutdown"));
                leader.run();
            }
        } catch (IllegalArgumentException | IOException exception) {
            System.err.println("Leader could not run: " + exception.getMessage());
            System.exit(1);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            System.err.println("Leader interrupted.");
        }
    }

    private void run() throws IOException, InterruptedException {
        Thread acceptor = new Thread(this::acceptConnections, "worker-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
        log("Listening on port " + server.getLocalPort() + "; round timeout " + timeoutSeconds + " seconds.");
        log("Need at least 3 connected workers. Commands: workers, quit.");
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (!closed.get()) {
            System.out.print("Task (for example 15 + 27), workers, or quit > ");
            System.out.flush();
            String task = console.readLine();
            if (task == null || task.trim().equalsIgnoreCase("quit")) {
                break;
            }
            task = task.trim();
            if (task.equalsIgnoreCase("workers")) {
                synchronized (stateLock) {
                    log("Connected workers (" + workers.size() + "): " + workers.keySet());
                }
            } else if (task.length() > 200 || !TASK.matcher(task).matches()) {
                log("Enter one addition, subtraction, or multiplication expression, such as 15 + 27.");
            } else {
                runRound(task);
            }
        }
    }

    private void acceptConnections() {
        while (!closed.get()) {
            try {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                socket.setSoTimeout(5000);
                readers.execute(() -> handleConnection(socket));
            } catch (IOException exception) {
                if (!closed.get()) {
                    log("Accept failed: " + exception.getMessage());
                    close();
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        WorkerConnection worker = null;
        try {
            worker = new WorkerConnection(socket);
            String hello = readProtocolLine(worker.input);
            String[] fields = hello == null ? new String[0] : hello.split("\\t", -1);
            if (fields.length != 2 || !fields[0].equals("HELLO") || !WORKER_ID.matcher(fields[1]).matches()) {
                worker.send("ERROR\t0\tFirst message must be HELLO followed by a valid worker ID.");
                return;
            }
            worker.id = fields[1];
            synchronized (stateLock) {
                if (closed.get()) {
                    return;
                }
                if (workers.containsKey(worker.id)) {
                    worker.send("ERROR\t0\tWorker ID is already connected.");
                    return;
                }
                if (!worker.send("WELCOME\t" + worker.id)) {
                    return;
                }
                workers.put(worker.id, worker);
                log(worker.id + " connected from " + socket.getRemoteSocketAddress()
                        + ". Connected workers: " + workers.size());
                if (activeRound != null) {
                    log(worker.id + " will participate starting with the next round.");
                }
            }
            socket.setSoTimeout(0);
            String message;
            while (!closed.get() && (message = readProtocolLine(worker.input)) != null) {
                log("RECV " + worker.id + ": " + readable(message));
                receiveResult(worker, message);
            }
        } catch (IOException exception) {
            if (!closed.get()) {
                log("Connection ended" + (worker == null || worker.id == null ? "" : " for " + worker.id)
                        + ": " + exception.getMessage());
            }
        } finally {
            if (worker != null) {
                worker.close();
            } else {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Ignore cleanup errors on this connection.
                }
            }
        }
    }

    private void runRound(String task) throws InterruptedException {
        Round round;
        synchronized (stateLock) {
            if (workers.size() < 3) {
                log("Cannot start: need at least 3 connected workers; currently " + workers.size() + ".");
                return;
            }
            round = new Round(nextRoundId++, new LinkedHashMap<>(workers), timeoutSeconds);
            activeRound = round;
        }
        log("Round " + round.id + " started. Participants: " + round.participants.keySet()
                + "; strict majority requires " + round.majority() + "/" + round.participants.size() + ".");
        for (WorkerConnection worker : round.participants.values()) {
            worker.send("TASK\t" + round.id + "\t" + timeoutSeconds + "\t" + task);
        }

        synchronized (stateLock) {
            while (!closed.get() && round.votes.size() < round.participants.size()) {
                long remaining = round.deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                TimeUnit.NANOSECONDS.timedWait(stateLock, remaining);
            }
            round.accepting = false;
            activeRound = null;
        }
        if (closed.get()) {
            return;
        }

        List<String> missing = new ArrayList<>(round.participants.keySet());
        missing.removeAll(round.votes.keySet());
        if (missing.isEmpty()) {
            log("Round " + round.id + ": all responses received.");
        } else {
            log("Round " + round.id + " timed out. No vote from: " + missing
                    + ". These workers still count in the denominator.");
        }
        Map<String, Integer> counts = round.counts();
        String winner = "-";
        int largestCount = 0;
        for (Map.Entry<String, Integer> vote : counts.entrySet()) {
            largestCount = Math.max(largestCount, vote.getValue());
            if (vote.getValue() >= round.majority()) {
                winner = vote.getKey();
            }
        }
        String status = winner.equals("-") ? "NO_CONSENSUS" : "CONSENSUS";
        log("Round " + round.id + " vote distribution: " + counts + "; responses "
                + round.votes.size() + "/" + round.participants.size() + ".");
        if (status.equals("CONSENSUS")) {
            log("Consensus reached: " + winner + " (" + largestCount + "/"
                    + round.participants.size() + " workers agreed).");
        } else {
            log("No consensus reached. Required " + round.majority() + "/" + round.participants.size() + ".");
        }
        String decision = "DECISION\t" + round.id + "\t" + status + "\t" + winner + "\t" + largestCount
                + "\t" + round.participants.size() + "\t" + round.votes.size() + "\t" + counts;
        List<WorkerConnection> recipients;
        synchronized (stateLock) {
            recipients = new ArrayList<>(workers.values());
        }
        for (WorkerConnection worker : recipients) {
            worker.send(decision);
        }
    }

    private void receiveResult(WorkerConnection worker, String message) {
        String[] fields = message.split("\\t", -1);
        if (fields.length != 4 || !fields[0].equals("RESULT")) {
            worker.send("ERROR\t0\tExpected RESULT, round ID, worker ID, and numeric answer.");
            return;
        }
        long roundId;
        try {
            roundId = Long.parseLong(fields[1]);
        } catch (NumberFormatException exception) {
            worker.send("ERROR\t0\tInvalid round ID.");
            return;
        }
        String error = null;
        String answer = null;
        synchronized (stateLock) {
            Round round = activeRound;
            if (!fields[2].equals(worker.id)) {
                error = "Worker ID does not match this connection.";
            } else if (round == null || round.id != roundId || !round.accepting) {
                error = "Round is not active; stale or future result ignored.";
            } else if (round.participants.get(worker.id) != worker) {
                error = "This connection is not a participant in this round.";
            } else if (round.votes.containsKey(worker.id)) {
                error = "Duplicate response ignored; only the first valid vote counts.";
            } else if (System.nanoTime() >= round.deadlineNanos) {
                error = "Deadline passed; late response ignored.";
            } else if (fields[3].length() > 100 || !NUMBER.matcher(fields[3]).matches()) {
                error = "Answer must be a decimal number with at most 100 characters.";
            } else {
                answer = new BigDecimal(fields[3]).stripTrailingZeros().toPlainString();
                round.votes.put(worker.id, answer);
                log("Round " + round.id + " counted " + worker.id + " = " + answer
                        + "; votes " + round.counts() + "; received " + round.votes.size()
                        + "/" + round.participants.size() + "; required " + round.majority() + ".");
                stateLock.notifyAll();
            }
        }
        if (error == null) {
            worker.send("ACK\t" + roundId + "\t" + worker.id + "\t" + answer);
        } else {
            log("Ignored response from " + worker.id + ": " + error);
            worker.send("ERROR\t" + roundId + "\t" + error);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            server.close();
        } catch (IOException ignored) {
            // The listener is shutting down; no retry is needed.
        }
        List<WorkerConnection> connections;
        synchronized (stateLock) {
            connections = new ArrayList<>(workers.values());
            stateLock.notifyAll();
        }
        for (WorkerConnection worker : connections) {
            worker.send("BYE\tLeader is shutting down.");
            worker.close();
        }
        readers.shutdownNow();
        log("Leader stopped.");
    }

    private static String readProtocolLine(BufferedReader input) throws IOException {
        StringBuilder line = new StringBuilder();
        int character;
        while ((character = input.read()) != -1) {
            if (character == '\n') {
                return line.toString();
            }
            if (character != '\r') {
                line.append((char) character);
                if (line.length() > 4096) {
                    throw new IOException("Protocol line exceeds 4096 characters.");
                }
            }
        }
        return line.length() == 0 ? null : line.toString();
    }

    private static void log(String message) {
        System.out.println("[Leader] " + message);
    }

    private static String readable(String message) {
        return message.replace("\t", " | ");
    }

    private final class WorkerConnection implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader input;
        private final BufferedWriter output;
        private final AtomicBoolean disconnected = new AtomicBoolean();
        private String id;

        private WorkerConnection(Socket socket) throws IOException {
            this.socket = socket;
            input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        private synchronized boolean send(String message) {
            if (disconnected.get()) {
                return false;
            }
            try {
                output.write(message);
                output.newLine();
                output.flush();
                log("SEND " + (id == null ? "unregistered worker" : id) + ": " + readable(message));
                return true;
            } catch (IOException exception) {
                log("Send failed for " + id + ": " + exception.getMessage());
                close();
                return false;
            }
        }

        @Override
        public void close() {
            if (!disconnected.compareAndSet(false, true)) {
                return;
            }
            try {
                socket.close();
            } catch (SocketException ignored) {
                // The peer may have already disconnected.
            } catch (IOException ignored) {
                // Still remove the worker from the connection list below.
            }
            synchronized (stateLock) {
                if (id != null && workers.get(id) == this) {
                    workers.remove(id);
                    log(id + " disconnected. Connected workers: " + workers.size());
                    if (activeRound != null && activeRound.participants.get(id) == this) {
                        log(id + " remains a participant in round " + activeRound.id + ".");
                    }
                    stateLock.notifyAll();
                }
            }
        }
    }

    private static final class Round {
        private final long id;
        private final Map<String, WorkerConnection> participants;
        private final Map<String, String> votes = new LinkedHashMap<>();
        private final long deadlineNanos;
        private boolean accepting = true;

        private Round(long id, Map<String, WorkerConnection> participants, int timeoutSeconds) {
            this.id = id;
            this.participants = participants;
            deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        }

        private int majority() {
            return participants.size() / 2 + 1;
        }

        private Map<String, Integer> counts() {
            Map<String, Integer> result = new LinkedHashMap<>();
            for (String value : votes.values()) {
                result.merge(value, 1, Integer::sum);
            }
            return result;
        }
    }
}
