package io.dragonfly.nifi.redis.util;

import io.dragonfly.nifi.redis.services.ReplicationEndpoint;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * A replica connection to a Redis or Valkey master: the PSYNC handshake, then the command stream
 * as raw argument lists. The RDB the master sends on a full resync is skipped, never parsed, so
 * any RDB version works (Redis 8 writes version 15, which no Java RDB parser reads today), and
 * {@code REPLCONF rdb-filter-only ""} asks the master to leave the data out of it. Offsets follow
 * Redis: {@link #offset()} is the last byte applied, and a resume asks for the byte after it.
 */
public final class ReplicationStream implements Closeable {

    public enum SyncKind { FULL, CONTINUE }

    public interface Listener {
        /** A command applied at {@code db}; {@code offsetAfter} counts its bytes. */
        void onCommand(int db, List<byte[]> argv, long offsetAfter);
    }

    private static final long ACK_INTERVAL_MS = 1000;
    private static final int READ_TIMEOUT_MS = 1000;
    private static final int MID_COMMAND_TIMEOUT_MS = 60_000;

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final SyncKind syncKind;
    private volatile String replId;
    private volatile long offset;
    private volatile boolean closed;
    private volatile int db;
    private long lastAckAtMs;

    private ReplicationStream(Socket socket, SyncKind syncKind, String replId, long offset, int db) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        this.out = socket.getOutputStream();
        this.syncKind = syncKind;
        this.replId = replId;
        this.offset = offset;
        this.db = db;
    }

    /**
     * Connects and completes the handshake. With a known {@code replId} it asks to continue after
     * {@code offset}; the master answers with a full resync instead when it cannot, which
     * {@link #syncKind()} reports. {@code db} is the database selected at {@code offset}: a
     * continued stream does not repeat the SELECT that came before it. {@code onSocket} receives
     * the socket before the handshake, so the caller can abort a connect, handshake or RDB
     * transfer from another thread by closing it.
     */
    public static ReplicationStream open(ReplicationEndpoint endpoint, String replId, long offset, int db, int connectTimeoutMs,
                                         java.util.function.Consumer<Closeable> onSocket) throws IOException {
        Socket socket = new Socket();
        onSocket.accept(socket);
        try {
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), connectTimeoutMs);
            if (endpoint.sslSocketFactory() != null) {
                SSLSocket tls = (SSLSocket) endpoint.sslSocketFactory().createSocket(socket, endpoint.host(), endpoint.port(), true);
                // A bare SSLSocketFactory checks the certificate chain but not that it names this host.
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                socket = tls;
            }
            socket.setSoTimeout(connectTimeoutMs);
            Handshake handshake = new Handshake(socket);
            if (endpoint.password() != null) {
                handshake.expectOk(endpoint.username() == null
                        ? command("AUTH", new String(endpoint.password()))
                        : command("AUTH", endpoint.username(), new String(endpoint.password())));
            }
            handshake.expectOk(command("PING"));
            handshake.expectOk(command("REPLCONF", "listening-port", "0"));
            handshake.expectOk(command("REPLCONF", "capa", "eof", "capa", "psync2"));
            // Redis 7.0+ and Valkey; older servers refuse it and send the full RDB, which is then
            // skipped all the same, only at the cost of the transfer.
            handshake.send(command("REPLCONF", "rdb-filter-only", ""));
            handshake.readLine();
            handshake.send(replId == null ? command("PSYNC", "?", "-1") : command("PSYNC", replId, String.valueOf(offset + 1)));
            String reply = handshake.readNonEmptyLine();
            String[] parts = reply.split(" ");
            ReplicationStream stream;
            if (parts[0].equals("+FULLRESYNC")) {
                // A replica's connection to its master starts in database 0, and a master that has already
                // selected 0 for another replica sends its next writes with no SELECT (Redis 8.10 does).
                stream = new ReplicationStream(socket, SyncKind.FULL, parts[1], Long.parseLong(parts[2]), 0);
                stream.skipRdb();
            } else if (parts[0].equals("+CONTINUE")) {
                stream = new ReplicationStream(socket, SyncKind.CONTINUE, parts.length > 1 ? parts[1] : replId, offset, db);
            } else {
                throw new IOException("PSYNC refused: " + reply);
            }
            socket.setSoTimeout(READ_TIMEOUT_MS);
            return stream;
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
    }

    public SyncKind syncKind() {
        return syncKind;
    }

    public String replId() {
        return replId;
    }

    public long offset() {
        return offset;
    }

    /** The database selected at {@link #offset()}: 0 after a full resync until a SELECT says otherwise. */
    public int db() {
        return db;
    }

    /** Reads commands until the connection fails or {@link #close()} is called; acknowledges the offset every second. */
    public void run(Listener listener) throws IOException {
        while (!closed) {
            ackIfDue();
            List<byte[]> argv;
            long size;
            try {
                CountingRead read = readCommand();
                argv = read.argv();
                size = read.bytes();
            } catch (SocketTimeoutException idle) {
                continue;
            }
            offset += size;
            if (argv.isEmpty()) {
                continue;
            }
            String name = new String(argv.get(0), StandardCharsets.US_ASCII).toLowerCase();
            if (name.equals("select") && argv.size() > 1) {
                db = Integer.parseInt(new String(argv.get(1), StandardCharsets.US_ASCII));
                listener.onCommand(db, List.of(), offset);
            } else if (name.equals("replconf") && argv.size() > 1
                    && new String(argv.get(1), StandardCharsets.US_ASCII).equalsIgnoreCase("getack")) {
                sendAck();
            } else {
                listener.onCommand(db, argv, offset);
            }
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        socket.close();
    }

    private void ackIfDue() throws IOException {
        if (System.currentTimeMillis() - lastAckAtMs >= ACK_INTERVAL_MS) {
            sendAck();
        }
    }

    private void sendAck() throws IOException {
        synchronized (out) {
            out.write(command("REPLCONF", "ACK", String.valueOf(offset)));
            out.flush();
        }
        lastAckAtMs = System.currentTimeMillis();
    }

    /** A disk-based transfer sends $length; a diskless one wraps the payload in a 40-byte EOF marker. */
    private void skipRdb() throws IOException {
        String header;
        do {
            header = readLine(in);
        } while (header.isEmpty());
        if (!header.startsWith("$")) {
            throw new IOException("Expected an RDB payload, got: " + header);
        }
        if (header.startsWith("$EOF:")) {
            byte[] marker = header.substring(5).getBytes(StandardCharsets.US_ASCII);
            byte[] tail = new byte[marker.length];
            int filled = 0;
            while (true) {
                int b = in.read();
                if (b < 0) {
                    throw new EOFException("Connection closed inside the RDB payload");
                }
                if (filled < tail.length) {
                    tail[filled++] = (byte) b;
                } else {
                    System.arraycopy(tail, 1, tail, 0, tail.length - 1);
                    tail[tail.length - 1] = (byte) b;
                }
                if (filled == tail.length && Arrays.equals(tail, marker)) {
                    return;
                }
            }
        }
        long remaining = Long.parseLong(header.substring(1));
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) {
                if (in.read() < 0) {
                    throw new EOFException("Connection closed inside the RDB payload");
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }

    private record CountingRead(List<byte[]> argv, long bytes) {
    }

    /**
     * The first byte is read with the socket's idle timeout; once a command has started, the rest
     * of it is read in full, so a timeout can only fall between commands and never loses bytes.
     */
    private CountingRead readCommand() throws IOException {
        int first = in.read();
        if (first < 0) {
            throw new EOFException("Master closed the replication connection");
        }
        if (first == '\n') {
            return new CountingRead(List.of(), 1);
        }
        socket.setSoTimeout(MID_COMMAND_TIMEOUT_MS);
        try {
            return readRestOfCommand(first);
        } catch (SocketTimeoutException stalled) {
            // Not idle: part of a command has been consumed, so resuming here would parse the rest
            // as new commands and count a wrong offset. Reconnecting resumes from the last whole one.
            throw new IOException("The master stalled in the middle of a command", stalled);
        } finally {
            socket.setSoTimeout(READ_TIMEOUT_MS);
        }
    }

    private CountingRead readRestOfCommand(int first) throws IOException {
        if (first != '*') {
            String inline = (char) first + readLine(in);
            return new CountingRead(List.of(inline.trim().getBytes(StandardCharsets.UTF_8)), inline.length() + 2L);
        }
        String countLine = readLine(in);
        long bytes = 1 + countLine.length() + 2;
        int count = Integer.parseInt(countLine);
        List<byte[]> argv = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String lengthLine = readLine(in);
            if (!lengthLine.startsWith("$")) {
                throw new IOException("Expected a bulk string, got: " + lengthLine);
            }
            int length = Integer.parseInt(lengthLine.substring(1));
            argv.add(in.readNBytes(length));
            if (in.read() != '\r' || in.read() != '\n') {
                throw new IOException("Malformed bulk string terminator");
            }
            bytes += lengthLine.length() + 2 + length + 2;
        }
        return new CountingRead(argv, bytes);
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException("Connection closed mid-line");
            }
            if (b == '\r') {
                if (in.read() != '\n') {
                    throw new IOException("Malformed line terminator");
                }
                return line.toString(StandardCharsets.UTF_8);
            }
            if (b == '\n') {
                return line.toString(StandardCharsets.UTF_8);
            }
            line.write(b);
        }
    }

    static byte[] command(String... args) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String arg : args) {
            byte[] raw = arg.getBytes(StandardCharsets.UTF_8);
            buffer.writeBytes(("$" + raw.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            buffer.writeBytes(raw);
            buffer.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return buffer.toByteArray();
    }

    private static final class Handshake {
        private final InputStream in;
        private final OutputStream out;

        Handshake(Socket socket) throws IOException {
            this.in = new BufferedInputStream(socket.getInputStream(), 1);
            this.out = socket.getOutputStream();
        }

        void send(byte[] command) throws IOException {
            out.write(command);
            out.flush();
        }

        String readLine() throws IOException {
            return ReplicationStream.readLine(in);
        }

        String readNonEmptyLine() throws IOException {
            String line;
            do {
                line = readLine();
            } while (line.isEmpty());
            return line;
        }

        void expectOk(byte[] command) throws IOException {
            send(command);
            String reply = readLine();
            if (reply.startsWith("-")) {
                throw new IOException("Replication handshake refused: " + reply);
            }
        }
    }
}
