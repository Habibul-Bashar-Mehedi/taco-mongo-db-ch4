package tacos.config;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CassandraKeyspaceConfig {

    @Bean
    @Primary
    public CqlSession cqlSession(
            @Value("${spring.cassandra.contact-points:127.0.0.1:9042}") List<String> contactPoints,
            @Value("${spring.cassandra.local-datacenter:datacenter1}") String localDatacenter,
            @Value("${spring.cassandra.keyspace-name:taco}") String keyspaceName,
            @Value("${spring.cassandra.port:9042}") int fallbackPort) {
        List<InetSocketAddress> resolvedContactPoints = resolveContactPoints(contactPoints, fallbackPort);
        CqlIdentifier keyspace = CqlIdentifier.fromCql(keyspaceName);

        try (CqlSession adminSession = CqlSession.builder()
                .addContactPoints(resolvedContactPoints)
                .withLocalDatacenter(localDatacenter)
                .build()) {
            String cql = "CREATE KEYSPACE IF NOT EXISTS " + keyspace.asCql(true)
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}";
            adminSession.execute(cql);
        }

        return CqlSession.builder()
                .addContactPoints(resolvedContactPoints)
                .withLocalDatacenter(localDatacenter)
                .withKeyspace(keyspace)
                .build();
    }

    private List<InetSocketAddress> resolveContactPoints(List<String> contactPoints, int fallbackPort) {
        List<InetSocketAddress> resolved = new ArrayList<>();
        if (contactPoints == null || contactPoints.isEmpty()) {
            resolved.add(new InetSocketAddress("127.0.0.1", fallbackPort));
            return resolved;
        }

        for (String contactPoint : contactPoints) {
            String host = contactPoint;
            int port = fallbackPort;
            int colon = contactPoint.lastIndexOf(':');
            if (colon > 0 && colon < contactPoint.length() - 1) {
                host = contactPoint.substring(0, colon);
                port = Integer.parseInt(contactPoint.substring(colon + 1));
            }
            resolved.add(new InetSocketAddress(host, port));
        }

        return resolved;
    }
}
