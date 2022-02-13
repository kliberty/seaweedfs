CREATE OR REPLACE FUNCTION fmeta_pgsql_ins ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
DECLARE
    flag boolean;
BEGIN
    INSERT INTO internal_filemeta (dirhash, name, directory, meta)
        VALUES (
            NEW.dirhash, 
            NEW.name, 
            NEW.directory, 
            NEW.meta
        )
        ON CONFLICT (dirhash, name)
            DO UPDATE SET
                meta = EXCLUDED.meta WHERE internal_filemeta.meta != EXCLUDED.meta
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
            AND name = NEW.name AND NOT
                (internal_json_meta.json_meta @> NEW.json_meta::jsonb AND internal_json_meta.json_meta <@ NEW.json_meta::jsonb);
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
    _metric text;
    _collection text;
BEGIN
    IF (NEW.json_meta ->> 'IsDirectory')::bool THEN
        _metric = 'directories';
    ELSE
        _metric = 'files';
    END IF;

    _collection = NEW.json_meta ->> 'Collection';

    CASE TG_OP
    WHEN 'INSERT' THEN
        UPDATE system_meta_counts
            SET counts = counts + 1
            WHERE metric = _metric AND collection = _collection;
    WHEN 'DELETE' THEN
        UPDATE system_meta_counts
            SET counts = counts - 1
            WHERE metric = _metric AND collection = _collection;
    END CASE;

    RETURN NULL;
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


-- SELECT pg_stat_statements_reset();
-- SELECT pg_stat_reset();

CREATE OR REPLACE FUNCTION reset_stats ()
    RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_stat_statements_reset();
    PERFORM pg_stat_reset();
END;
$$;

CREATE OR REPLACE FUNCTION collection_stats()
    RETURNS TABLE (collection TEXT, "size" numeric)
    LANGUAGE plpgsql 
    AS $$
DECLARE
    labels TEXT[] = ARRAY['topaz', 'seaweed-image-cache'];
    item TEXT;
BEGIN
	FOREACH item IN ARRAY labels LOOP
		RETURN QUERY
            SELECT item, sum((json_meta ->> 'FileSize')::bigint) AS topaz 
                FROM filemeta WHERE json_meta @> jsonb_build_object('Collection', item) ;
	END LOOP;
RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION fancy_list2(dir TEXT)
    RETURNS TABLE (directory TEXT, name TEXT, isdirectory BOOL, child BOOL)
    LANGUAGE plpgsql 
    AS $$
DECLARE
BEGIN
	RETURN QUERY SELECT directory, name, false, false FROM filemeta WHERE directory = dir AND (json_meta ->> 'IsDirectory')::bool = false ORDER BY name;

	FOR item IN (SELECT name FROM filemeta WHERE directory = dir AND (json_meta ->> 'IsDirectory')::bool = true ORDER BY name)
		RETURN QUERY
            SELECT directory, name, (json_meta ->> 'IsDirectory')::bool, true
                FROM filemeta WHERE directory = dir || '/' || item.name;
	END LOOP;
RETURN;
END;
$$;


CREATE OR REPLACE FUNCTION fancy_list1(dir TEXT)
    RETURNS TABLE (directory TEXT, name TEXT, isdirectory BOOL, child BOOL)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN WITH sub1 AS
    (SELECT dirhash,
            directory,
            name,
            isdirectory
     FROM filemeta
     WHERE directory = dir)
    (SELECT sub1.directory,
            sub1.name,
            sub1.isdirectory,
            false as child
     FROM sub1
     ORDER BY sub1.isdirectory,
              sub1.directory,
              sub1.name ASC)
UNION ALL
    (SELECT
        DISTINCT ON (f1.isdirectory, f1.directory)
        f1.directory,
        f1.name,
        f1.isdirectory,
        true as child
     FROM sub1
         JOIN filemeta f1 ON f1.directory = (sub1.directory || '/' || sub1.name)
     WHERE 
         f1.isdirectory = FALSE
         AND sub1.isdirectory = TRUE
     ORDER BY f1.isdirectory,
              f1.directory,
              f1.name ASC);
END;
$$;