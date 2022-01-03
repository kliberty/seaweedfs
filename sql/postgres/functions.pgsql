CREATE OR REPLACE FUNCTION fmeta_pgsql_ins ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
DECLARE
    flag boolean;
BEGIN
    INSERT INTO internal_filemeta (dirhash, name, directory, meta, isdirectory, filesize)
        VALUES (
            NEW.dirhash, 
            NEW.name, 
            NEW.directory, 
            NEW.meta, 
            (NEW.jon_meta::jsonb ->> 'IsDirectory')::bool, 
            (NEW.json_meta::jsonb ->> 'FileSize')::bigint
        )
        ON CONFLICT (dirhash, name)
            DO UPDATE SET
                meta = EXCLUDED.meta, isdirectory = (NEW.json_meta::jsonb ->> 'IsDirectory')::bool, filesize = (NEW.json_meta::jsonb ->> 'FileSize')::bigint
        RETURNING (xmax = 0) INTO flag;
    -- We already know if there is a conflict, no need to use an UPSERT again
    IF flag THEN
        INSERT INTO internal_json_meta (dirhash, name, json_meta)
            VALUES (
                NEW.dirhash, 
                NEW.name, 
                NEW.json_meta::jsonb
            );
    ELSE
        -- RAISE WARNING 'Updating internal json';
        UPDATE
            internal_json_meta
        SET json_meta = NEW.json_meta::jsonb
        WHERE dirhash = NEW.dirhash
            AND name = NEW.name;
    END IF;
    RETURN NEW;
END;
$$;

-- CREATE OR REPLACE FUNCTION fmeta_pgsql_del ()
--     RETURNS TRIGGER
--     LANGUAGE plpgsql
--     AS $$
-- BEGIN
--     DELETE FROM internal_filemeta
--     WHERE dirhash = OLD.dirhsh
--         AND name = OLD.name;
-- END;
-- $$;

CREATE OR REPLACE FUNCTION update_counts ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
DECLARE
    metric text;
BEGIN
    --	RAISE warning 'Inserting json';
    IF (NEW.json_meta ->> 'IsDirectory')::bool THEN
        metric = 'directories';
    ELSE
        metric = 'files';
    END IF;
    CASE TG_OP
    WHEN 'INSERT' THEN
        PERFORM increase_count (metric);
    WHEN 'DELETE' THEN
        PERFORM decrease_count (metric);
    END CASE;
    RETURN NULL;
EXCEPTION
    WHEN OTHERS THEN
    RAISE warning 'Caught'; RETURN NULL;
    END;
$$;

CREATE OR REPLACE FUNCTION increase_count (_metric text)
    RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    r bigint;
BEGIN
    UPDATE
        system_meta_counts
    SET counts = counts + 1
    WHERE metric = _metric
    RETURNING counts INTO r;
    RETURN r;
END;
$$;

CREATE OR REPLACE FUNCTION decrease_count (_metric text)
    RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    r bigint;
BEGIN
    UPDATE
        system_meta_counts
    SET counts = counts - 1
    WHERE metric = _metric
    RETURNING counts INTO r;
    RETURN r;
END;
$$;

CREATE TABLE system_meta_counts (
    metric text PRIMARY KEY, counts bigint
);

-- SELECT pg_stat_statements_reset();
-- SELECT pg_stat_reset();

CREATE OR REPLACE FUNCTION reset_stats ()
    RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    SELECT pg_stat_statements_reset();
    SELECT pg_stat_reset();
END;
$$;