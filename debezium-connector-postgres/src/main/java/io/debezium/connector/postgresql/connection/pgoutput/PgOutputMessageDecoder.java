/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</a>.
 * Only one message is delivered for processing.
 *
 * @author Jiri Pechanec
 *
 */
public class PgOutputMessageDecoder implements MessageDecoder {

    @Override
    public void processMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {

        String type = new String(new byte[] { buffer.get() });

        if (type.equals("I")) {
            System.out.println("### Received INSERT event");
            int relationId = buffer.getInt();
            char tupelDataType = (char) buffer.get();
            short numberOfColumns = buffer.getShort();

            for(int i = 0; i < numberOfColumns; i++) {
                char dataType = (char) buffer.get();
                if (dataType == 't') {
                    int valueLength = buffer.getInt();
                    byte[] value = new byte[valueLength];
                    buffer.get(value, 0, valueLength);
                    String valueStr = new String(value);

                    System.out.println(valueStr);
                }
            }
        }
        else if (type.equals("B")) {
            System.out.println("### Received BEGIN event");
        }
        else if (type.equals("C")) {
            System.out.println("### Received COMMIT event");
        }
        else {
            System.out.println("### Received event: " + type);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return builder.withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", "dbz_publication");
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder tryOnceOptions(ChainedLogicalStreamBuilder builder) {
        return builder;
    }
}
