package telegram.files.repository;

import cn.hutool.core.lang.Version;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import telegram.files.Config;

import java.util.Map;
import java.util.TreeMap;

public record SettingRecord(String key, String value) {

    public static final String KEY_FIELD = Config.isMysql() ? "`key`" : "key";

    public static final String SCHEME = """
            CREATE TABLE IF NOT EXISTS setting_record
            (
                %s      VARCHAR(255) PRIMARY KEY,
                value   TEXT
            )
            """.formatted(KEY_FIELD);

    public static class SettingRecordDefinition implements Definition {
        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public TreeMap<Version, String[]> getMigrations() {
            TreeMap<Version, String[]> migrations = new TreeMap<>();
            
            // DESIGN NOTE: Migration to fix corrupted automation states caused by wrong BitState constants
            // Reset all automation states to 0 to recover from invalid bit combinations
            migrations.put(new Version("1.0.0"), new String[]{
                Config.isPostgres() ? """
                    UPDATE setting_record 
                    SET value = jsonb_set(
                        value::jsonb, 
                        '{automations}',
                        (
                            SELECT jsonb_agg(
                                jsonb_set(a, '{state}', '0'::jsonb)
                            )
                            FROM jsonb_array_elements(value::jsonb->'automations') a
                        )
                    )
                    WHERE key = 'automation' AND value::jsonb ? 'automations'
                    """ :
                Config.isMysql() ? """
                    UPDATE setting_record 
                    SET value = JSON_SET(
                        value,
                        '$.automations',
                        JSON_ARRAY()
                    )
                    WHERE `key` = 'automation' AND JSON_VALID(value) = 1 AND JSON_EXTRACT(value, '$.automations') IS NOT NULL;
                    
                    UPDATE setting_record 
                    SET value = (
                        SELECT JSON_ARRAYAGG(
                            JSON_SET(automation, '$.state', 0)
                        )
                        FROM JSON_TABLE(
                            JSON_EXTRACT(value, '$.automations'),
                            '$[*]' COLUMNS (
                                automation JSON PATH '$'
                            )
                        ) AS jt
                    )
                    WHERE `key` = 'automation' AND JSON_VALID(value) = 1 AND JSON_EXTRACT(value, '$.automations') IS NOT NULL
                    """ : """
                    UPDATE setting_record 
                    SET value = json_set(
                        value,
                        '$.automations',
                        json_group_array(
                            json_set(json_extract(automation.value, '$'), '$.state', 0)
                        )
                    )
                    FROM (
                        SELECT json_extract(sr.value, '$.automations') as automations_array
                        FROM setting_record sr 
                        WHERE sr.key = 'automation' AND json_valid(sr.value) = 1
                    ) AS source,
                    json_each(source.automations_array) AS automation
                    WHERE key = 'automation' AND json_valid(value) = 1 AND json_extract(value, '$.automations') IS NOT NULL
                    """
            });
            
            return migrations;
        }
    }

    public static RowMapper<SettingRecord> ROW_MAPPER = row ->
            new SettingRecord(row.getString("key"),
                    row.getString("value")
            );

    public static TupleMapper<SettingRecord> PARAM_MAPPER = TupleMapper.mapper(r ->
            Map.ofEntries(Map.entry("key", r.key()),
                    Map.entry("value", r.value())
            ));
}
