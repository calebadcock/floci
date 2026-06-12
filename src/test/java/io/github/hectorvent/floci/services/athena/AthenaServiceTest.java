package io.github.hectorvent.floci.services.athena;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.floci.FlociDuckClient;
import io.github.hectorvent.floci.services.glue.GlueService;
import io.github.hectorvent.floci.services.glue.model.StorageDescriptor;
import io.github.hectorvent.floci.services.glue.model.Table;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AthenaServiceTest {

    private GlueService glueService;
    private AthenaService athenaService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        StorageFactory storageFactory = mock(StorageFactory.class);
        when(storageFactory.create(anyString(), anyString(), any(TypeReference.class)))
                .thenReturn(mock(StorageBackend.class));
        glueService = mock(GlueService.class);
        athenaService = new AthenaService(storageFactory, mock(FlociDuckClient.class), glueService,
                null, mock(EmulatorConfig.class), mock(Vertx.class));
    }

    @Test
    void parquetTableGeneratesReadParquetView() {
        when(glueService.getTables("analytics")).thenReturn(List.of(
                table("orders", "s3://lake/orders/", "parquet-input-format", null)));

        String ddl = athenaService.buildGlueDdl("analytics");

        assertTrue(ddl.contains(
                "CREATE OR REPLACE VIEW \"orders\" AS SELECT * FROM read_parquet('s3://lake/orders/**'"));
        assertFalse(ddl.contains("iceberg"));
    }

    @Test
    void icebergTableGeneratesIcebergScanFromMetadataLocation() {
        when(glueService.getTables("analytics")).thenReturn(List.of(
                table("orders", "s3://lake/orders/", null, Map.of(
                        "table_type", "ICEBERG",
                        "metadata_location", "s3://lake/orders/metadata/00001.metadata.json"))));

        String ddl = athenaService.buildGlueDdl("analytics");

        assertTrue(ddl.startsWith("INSTALL iceberg; LOAD iceberg;"));
        assertTrue(ddl.contains("CREATE OR REPLACE VIEW \"orders\" AS SELECT * FROM "
                + "iceberg_scan('s3://lake/orders/metadata/00001.metadata.json', allow_moved_paths = true)"));
    }

    @Test
    void icebergTableTypeIsCaseInsensitive() {
        when(glueService.getTables("analytics")).thenReturn(List.of(
                table("orders", "s3://lake/orders/", null, Map.of(
                        "table_type", "iceberg",
                        "metadata_location", "s3://lake/orders/metadata/00001.metadata.json"))));

        String ddl = athenaService.buildGlueDdl("analytics");

        assertTrue(ddl.contains("iceberg_scan('s3://lake/orders/metadata/00001.metadata.json'"));
    }

    @Test
    void icebergTableWithoutMetadataLocationIsSkipped() {
        when(glueService.getTables("analytics")).thenReturn(List.of(
                table("orders", "s3://lake/orders/", null, Map.of("table_type", "ICEBERG")),
                table("events", "s3://lake/events/", "parquet-input-format", null)));

        String ddl = athenaService.buildGlueDdl("analytics");

        assertFalse(ddl.contains("iceberg"), "Unbindable Iceberg table must not poison the database DDL");
        assertTrue(ddl.contains("read_parquet('s3://lake/events/**'"));
    }

    @Test
    void mixedTablesLoadIcebergExtensionOnce() {
        when(glueService.getTables("analytics")).thenReturn(List.of(
                table("orders", "s3://lake/orders/", "parquet-input-format", null),
                table("events", "s3://lake/events/", null, Map.of(
                        "table_type", "ICEBERG",
                        "metadata_location", "s3://lake/events/metadata/00002.metadata.json"))));

        String ddl = athenaService.buildGlueDdl("analytics");

        assertTrue(ddl.startsWith("INSTALL iceberg; LOAD iceberg;"));
        assertTrue(ddl.contains("read_parquet('s3://lake/orders/**'"));
        assertTrue(ddl.contains("iceberg_scan('s3://lake/events/metadata/00002.metadata.json'"));
        assertEquals(ddl.indexOf("INSTALL iceberg"), ddl.lastIndexOf("INSTALL iceberg"));
    }

    private static Table table(String name, String location, String inputFormat, Map<String, String> parameters) {
        Table table = new Table();
        table.setName(name);
        table.setParameters(parameters);
        if (location != null || inputFormat != null) {
            StorageDescriptor descriptor = new StorageDescriptor();
            descriptor.setLocation(location);
            descriptor.setInputFormat(inputFormat);
            table.setStorageDescriptor(descriptor);
        }
        return table;
    }
}
