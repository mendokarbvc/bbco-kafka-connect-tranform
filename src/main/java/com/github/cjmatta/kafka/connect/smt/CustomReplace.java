package com.github.cjmatta.kafka.connect.smt;


import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
//import org.apache.kafka.connect.transforms.util.SchemaUtil.SchemaBuilder;
//import org.apache.kafka.connect.version.Versioned;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public abstract class CustomReplace<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(CustomReplace.class);
    public static final String OVERVIEW_DOC = "Rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    // 1 DEBEMOS CONFIGURAR LAS LOS NOMBRES DE VARIABLES A USAR         
    interface ConfigName {
        String RENAME = "renames";
        String ORIGIN = "origin";
        String DESTINY = "destiny";
    }

    // 2 DEBEMOS INICIAR LA VARIABLE E INDICAR EL TIPO DE DATO Y SU RESPECTIVA VALIDACION
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            /* .define(ConfigName.RENAME, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
                @SuppressWarnings("unchecked")
                @Override
                public void ensureValid(String name, Object value) {
                    parseRenameMappings((List<String>) value);
                }

                @Override
                public String toString() {
                    return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                }
            }, ConfigDef.Importance.MEDIUM, "Field rename mappings.")*/

            //CONFIGURACION ORIGIN

            .define(ConfigName.ORIGIN, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Origin field")


            //CONFIGURACION DESTINY

            .define(ConfigName.DESTINY, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Destiny field");





    private static final String PURPOSE = "field replacement destiny to origin";

    private Map<String, String> renames;
    private Map<String, String> reverseRenames;

    //INICIAMOS EL ORIGIN EN UN MAP
    private Map<String, String> origins;
    private Map<String, String> reverseOrigin;

    //INICIAMOS EL DESTINY EN UN MAP
    private Map<String, String> destiny;
    private Map<String, String> reverseDestiny;

    private Cache<Schema, Schema> schemaUpdateCache;


    private String destinies;
    private String originies;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

        log.debug("Inverted origins: {}", configs);

        this.destinies = config.getString(ConfigName.DESTINY);
        this.originies = config.getString(ConfigName.ORIGIN);

        // Imprimir las configuraciones proporcionadas
        configs.forEach((k, v) -> log.debug("Provided Config Name: {}, Config Value: {}", k, v));




        // Obtener configuraciones de la base de datos
        List<String> origindb = getConfigsOrigin();
        origindb.forEach((k) -> log.debug("Config Name Origin: {}, Config Value: {}", k));


        List<String> destinydb = getConfigsDestiny();
        destinydb.forEach((k) -> log.debug("Config Name Destiny: {}, Config Value: {}", k));



        
        log.debug("ConfigName.ORIGIN: {}", ConfigName.ORIGIN);
        log.debug("config.getList(ConfigName.ORIGIN): {}", config.getString(ConfigName.ORIGIN));
        origins = parseOriginMappings((origindb));
        log.debug("Parsed origins: {}", origins);

        reverseOrigin = invert(origins);
        log.debug("Inverted origins: {}", reverseOrigin);

        destiny = parseDestinyMappings(destinydb);
        log.debug("Parsed destinys: {}", destiny);

        reverseDestiny = invert(destiny);
        log.debug("Inverted destinys: {}", reverseDestiny);


        renames = parseRenameMappings(origins, destiny);
        log.debug("Parsed renames: {}", destiny);
        reverseRenames = invert(renames);

        log.debug("Inverted reverseRenames: {}", reverseRenames);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
}

/*static Map<String, String> parseRenameMappings(List<String> mappings) {
    final Map<String, String> m = new HashMap<>();
    log.debug("Starting parseRenameMappings with mappings: {}", mappings);

    for (String mapping : mappings) {
        log.debug("Processing mapping: {}", mapping);
        final String[] parts = mapping.split(":");
        
        if (parts.length != 2) {
            log.error("Invalid rename mapping: {}", mapping);
            throw new ConfigException(ConfigName.RENAME, mappings, "Invalid rename mapping: " + mapping);
        }

        log.debug("Adding mapping: {} -> {}", parts[0], parts[1]);
        m.put(parts[0], parts[1]);
    }

    log.debug("Completed parseRenameMappings. Result: {}", m);
    return m;
}
*/

    static Map<String, String> parseOriginMappings(List<String> origins) {
        log.debug("Inside method :: parseOriginMappings :: {}", origins.toString());
        final Map<String, String> m = new HashMap<>();
        for (String origin : origins) {
            log.debug("Processing origin: {}", origin);  // Imprimir cada origen
            if (origin == null || origin.isEmpty()) {
                throw new ConfigException("Each origin must be a non-empty string.");
            }
            // Suponiendo que cada origen se mapeará a algo, actualiza aquí según sea necesario
            m.put(origin, "mappedValue");  // Ajusta según la lógica real
        }
        return m;
    }


    public static Map<String, String> parseRenameMappings(Map<String, String> origin, Map<String, String> destination) {
        final Map<String, String> m = new HashMap<>();
        log.debug("Starting parseRenameMappings with origin: {} and destination: {}", origin, destination);

        // Iterar sobre cada entrada del mapa de origen
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            for (Map.Entry<String, String> des : destination.entrySet()) {
            String originalKey = entry.getKey();
            String originalValue = entry.getValue();

            String desKey = des.getKey();
            String desValue = des.getValue();

            // Buscar la clave correspondiente en el mapa de destino
                log.debug("Adding mapping: {} -> {}", originalKey, desKey);
                //m.put(originalValue, destinationValue);

            String[] originalKeys = originalKey.split(",");
            String[] desKeys = desKey.split(",");

            for (int i = 0; i < originalKeys.length; i++) {
                m.put(originalKeys[i], desKeys[i]);
            }
            log.debug("VIEW mapping: {}", m);
        }}

        return m;
    }
    
    static Map<String, String> parseDestinyMappings(List<String> destinys) {
        log.debug("Inside method :: parseDestinyMappings :: {}", destinys.toString());
        final Map<String, String> m = new HashMap<>();
        for (String destiny : destinys) {
            log.debug("Processing destiny: {}", destiny);  // Imprimir cada destino
            if (destiny == null || destiny.isEmpty()) {
                throw new ConfigException("Each destiny must be a non-empty string.");
            }
            // Suponiendo que cada destino se mapeará a algo, actualiza aquí según sea necesario
            m.put(destiny, "mappedValue");  // Ajusta según la lógica real
        }
        return m;
    }

    static Map<String, String> invert(Map<String, String> source) {
        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    String renamed(String fieldName) {
        final String mapping = renames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    String reverseRenamed(String fieldName) {
        final String mapping = reverseRenames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        log.debug("Inside method :: originalRecord1 :: {}", value.toString());
        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldValue = e.getValue();
            updatedValue.put(renamed(fieldName), fieldValue);
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        log.debug("Inside method :: originalRecord2 :: {}", value.toString());
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(reverseRenamed(field.name()));
            updatedValue.put(field.name(), fieldValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(renamed(field.name()), field.schema());
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }


    private Connection connectToPostgres() throws SQLException {
    String url = "jdbc:postgresql://localhost:5432/postgres";
    Properties props = new Properties();
    props.setProperty("user", "postgres");
    props.setProperty("password", "postgres");
    return DriverManager.getConnection(url, props);
    
    }

    private List<String> getConfigsOrigin() {
        List<String> configs = new ArrayList<>();
        try {
            
        String query = "SELECT config_name, config_value FROM configs where config_name = '" + originies + "'";
        log.debug("Executing query: {}", query);
    
        try (Connection conn = connectToPostgres();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String configName = rs.getString("config_name");
                String configValue = rs.getString("config_value");
                configs.add(configValue);
            }
        } catch (SQLException e) {
            log.error("Failed to execute query on PostgreSQL database!", e);
        }
    
        log.debug("Retrieved {} configurations from database", configs.size());
        //return configs;
        } catch (Exception e) {
            log.error("Failed to execute getConfigsFromDatabase", e);
        }
        return configs;
       
        
    }


    private List<String> getConfigsDestiny() {
        List<String> configs = new ArrayList<>();
        try {
            String query = "SELECT config_name, config_value FROM configs WHERE config_name = '" + destinies + "'";
            log.debug("Executing query: {}", query);

            try (Connection conn = connectToPostgres();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    String configName = rs.getString("config_name");
                    String configValue = rs.getString("config_value");
                    configs.add((configValue));
                }
            } catch (SQLException e) {
                log.error("Failed to execute query on PostgreSQL database!", e);
            }

            log.debug("Retrieved {} configurations from database", configs.size());
        } catch (Exception e) {
            log.error("Failed to execute getConfigsFromDatabase", e);
        }
        return configs;
    }
    



    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CustomReplace<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends CustomReplace<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
