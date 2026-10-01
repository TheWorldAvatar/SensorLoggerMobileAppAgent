package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.net.URI;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.TimeUnit;

import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;

/** Verifies timeline access tokens issued by Keycloak. */
class TimelineAuthentication {
    private JwkProvider keys;
    private String issuer;

    private synchronized void configure() throws Exception {
        if (keys != null) {
            return;
        }

        String keycloakServer = System.getenv("KEYCLOAK_SERVER");
        String keycloakRealm = System.getenv("KEYCLOAK_REALM");
        if (keycloakServer == null || keycloakServer.isBlank()
                || keycloakRealm == null || keycloakRealm.isBlank()) {
            throw new IllegalStateException("Keycloak authentication is not configured");
        }

        issuer = keycloakServer.replaceAll("/+$", "") + "/realms/" + keycloakRealm;
        keys = new JwkProviderBuilder(URI.create(issuer + "/protocol/openid-connect/certs").toURL())
                .cached(10, 24, TimeUnit.HOURS)
                .rateLimited(10, 1, TimeUnit.MINUTES)
                .timeouts(5000, 5000)
                .build();
    }

    String authenticate(String header) {
        if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)
                || header.substring(7).isBlank()) {
            throw new AuthenticationException("Bearer token is missing");
        }

        try {
            configure();
            String token = header.substring(7).strip();
            DecodedJWT unverified = JWT.decode(token);
            if (!"RS256".equals(unverified.getAlgorithm()) || unverified.getKeyId() == null) {
                throw new IllegalArgumentException("Unsupported token signing header");
            }

            RSAPublicKey key = (RSAPublicKey) keys.get(unverified.getKeyId()).getPublicKey();
            DecodedJWT verified = JWT.require(Algorithm.RSA256(key, null))
                    .withIssuer(issuer)
                    .withClaimPresence("exp")
                    .withClaimPresence("sub")
                    .build()
                    .verify(token);
            String subject = verified.getSubject();
            if (subject == null || subject.isBlank()) {
                throw new IllegalArgumentException("Missing subject");
            }
            return subject;
        } catch (Exception e) {
            throw new AuthenticationException("Invalid bearer token or Keycloak authentication unavailable", e);
        }
    }

    static class AuthenticationException extends RuntimeException {
        AuthenticationException(String message) {
            super(message);
        }

        AuthenticationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
