package io.dragonfly.nifi.redis.services;

import javax.net.ssl.SSLSocketFactory;

/** Where a replica connection to the source's current master goes. {@code sslSocketFactory} is null without TLS. */
public record ReplicationEndpoint(String host, int port, String username, char[] password, int database,
                                  SSLSocketFactory sslSocketFactory) {
}
