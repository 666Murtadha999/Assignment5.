import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public final class Worker implements AutoCloseable {
    private static final Pattern WORKER_ID = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)");

    private final String id;
    private final Socket socket;
    private final BufferedReader input;
    private final BufferedWriter output;
    private final Object roundLock = new Object();
    private volatile boolean running = true;
    private PendingTask pending;

    private Worker(String id, String host, int port) throws IOException {
        this.id = id;
        socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);
            input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException exception) {
            socket.close();
            throw exception;
        }
    }

    public static void main(String[] args) {
        if (args.length != 3 || !WORKER_ID.matcher(args[0]).matches()) {
            System.err.println("Usage: Worker <workerId> <leaderHost> <port>");
            System.err.println("Worker ID: 1-32 letters, digits, underscores, or hyphens.");
            System.exit(1);
        }
        try {
            int port = Integer.parseInt(args[2]);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be 1-65535.");
            }
            try (Worker worker = new Worker(args[0], args[1], port)) {
                Runtime.getRuntime().addShutdownHook(new Thread(worker::close, "worker-shutdown"));
                worker.run();
            }
        } catch (IllegalArgumentException | IOException exception) {
            System.err.println("Worker could not connect or run: " + exception.getMessage());
            System.exit(1);
        }
    }

    private void run() throws IOException {
        send("HELLO\t" + id);
        String welcome = readProtocolLine();
        if (!("WELCOME\t" + id).equals(welcome)) {
            throw new IOException(welcome == null ? "Leader closed the connection during registration."
                    : welcome.replace("\t", " | "));
        }
        socket.setSoTimeout(0);
        log("Connected. Enter each answer manually.");
        log("Enter roundId:answer when prompted, or quit to disconnect.");
        Thread console = new Thread(this::readConsole, "manual-result-input");
        console.setDaemon(true);
        console.start();

        try {
            String message;
            while (running && (message = readProtocolLine()) != null) {
                log("RECV " + message.replace("\t", " | "));
                handleMessage(message);
            }
            if (running) {
                log("Leader disconnected.");
            }
        } catch (IOException exception) {
            if (running) {
                log("Disconnected from leader: " + exception.getMessage());
            }
        } finally {
            close();
            log("Worker stopped.");
        }
    }

    private void handleMessage(String message) throws IOException {
        String[] fields = message.split("\\t", -1);
        try {
            switch (fields[0]) {
                case "TASK":
                    if (fields.length != 4) {
                        throw new IOException("Malformed TASK message.");
                    }
                    long roundId = Long.parseLong(fields[1]);
                    synchronized (roundLock) {
                        pending = new PendingTask(roundId);
                    }
                    log("Round " + roundId + " task: " + fields[3] + "; leader timeout " + fields[2] + " seconds.");
                    log("Enter " + roundId + ":<your numeric result> (include the round ID).");
                    break;
                case "ACK":
                    log("Leader counted your response for round " + fields[1] + ": " + fields[3]);
                    break;
                case "DECISION":
                    if (fields.length != 8) {
                        throw new IOException("Malformed DECISION message.");
                    }
                    long decidedRound = Long.parseLong(fields[1]);
                    synchronized (roundLock) {
                        if (pending != null && pending.id == decidedRound) {
                            pending = null;
                        }
                    }
                    if (fields[2].equals("CONSENSUS")) {
                        log("Consensus reached for round " + decidedRound + ": " + fields[3]
                                + " (" + fields[4] + "/" + fields[5] + " workers agreed).");
                    } else {
                        log("No consensus reached for round " + decidedRound + ".");
                    }
                    log("Vote distribution: " + fields[7] + "; responses " + fields[6] + "/" + fields[5] + ".");
                    log("Waiting for the next task.");
                    break;
                case "ERROR":
                    log("Leader rejected the message: " + fields[fields.length - 1]);
                    break;
                case "BYE":
                    log("Leader is shutting down.");
                    close();
                    break;
                default:
                    log("Unrecognized leader message.");
            }
        } catch (NumberFormatException | IndexOutOfBoundsException exception) {
            throw new IOException("Malformed leader message.", exception);
        }
    }

    private void readConsole() {
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        try {
            String line;
            while (running && (line = console.readLine()) != null) {
                line = line.trim();
                if (line.equalsIgnoreCase("quit")) {
                    log("Disconnecting.");
                    close();
                    return;
                }
                int separator = line.indexOf(':');
                if (separator < 1) {
                    log("Use roundId:answer, for example 1:42 for round 1.");
                    continue;
                }
                try {
                    long roundId = Long.parseLong(line.substring(0, separator).trim());
                    String answer = line.substring(separator + 1).trim();
                    if (answer.length() > 100 || !NUMBER.matcher(answer).matches()) {
                        log("Enter a decimal number with at most 100 characters after the colon.");
                        continue;
                    }
                    // Normalize the typed number without calculating the task.
                    answer = new BigDecimal(answer).stripTrailingZeros().toPlainString();
                    synchronized (roundLock) {
                        if (pending == null || pending.id != roundId) {
                            log("Round " + roundId + " is not active here. Use the current round ID.");
                        } else if (pending.submitted) {
                            log("You already submitted a result for round " + roundId + ".");
                        } else {
                            send("RESULT\t" + roundId + "\t" + id + "\t" + answer);
                            pending.submitted = true;
                            log("Result submitted for round " + roundId + ": " + answer);
                        }
                    }
                } catch (NumberFormatException exception) {
                    log("The round ID must be an integer.");
                }
            }
            if (running) {
                log("Console input ended. Disconnecting.");
                close();
            }
        } catch (IOException exception) {
            if (running) {
                log("Input or connection failed: " + exception.getMessage());
                close();
            }
        }
    }

    private synchronized void send(String message) throws IOException {
        output.write(message);
        output.newLine();
        output.flush();
        log("SEND " + message.replace("\t", " | "));
    }

    private String readProtocolLine() throws IOException {
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

    private void log(String message) {
        System.out.println("[" + id + "] " + message);
    }

    @Override
    public void close() {
        running = false;
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing the socket also wakes the network reader.
        }
    }

    private static final class PendingTask {
        private final long id;
        private boolean submitted;

        private PendingTask(long id) {
            this.id = id;
        }
    }
}
