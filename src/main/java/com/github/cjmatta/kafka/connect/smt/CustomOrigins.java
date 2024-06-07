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

import java.util.*;

public abstract class CustomOrigins<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC = "Rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

            interface ConfigName {
                String ORIGIN = "origins11";
            }
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.ORIGIN, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
        @SuppressWarnings("unchecked")
        @Override
        public void ensureValid(String name, Object value) {
            parseOriginMappings((List<String>) value);
        }

        @Override
        public String toString() {
            return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
        }
    }, ConfigDef.Importance.MEDIUM, "Field origin mappings.");

    private static final String PURPOSE = "field replacement";

    private Map<String, String> origins;
    private Map<String, String> reverseOrigins;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    
        origins = parseOriginMappings(config.getList(ConfigName.ORIGIN));
        reverseOrigins = invert(origins);
    
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    
        // Logging para depuración
        System.out.println("Configuración de origins: " + origins);
    }

    static Map<String, String> parseOriginMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ConfigName.ORIGIN, mappings, "Invalid origin mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
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

    String originOf(String fieldName) {
        final String mapping = origins.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    String reverseOriginOf(String fieldName) {
        final String mapping = reverseOrigins.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            System.out.println("Valor operativo es nulo para el registro: " + record);
            return record;
        } else if (operatingSchema(record) == null) {
            System.out.println("Aplicando transformación sin esquema para el registro: " + record);
            return applySchemaless(record);
        } else {
            System.out.println("Aplicando transformación con esquema para el registro: " + record);
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldValue = e.getValue();
            updatedValue.put(originOf(fieldName), fieldValue);
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(reverseOriginOf(field.name()));
            updatedValue.put(field.name(), fieldValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(originOf(field.name()), field.schema());
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

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CustomOrigins<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends CustomOrigins<R> {

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
