CREATE EXTENSION pg_trgm;

-- DROP TABLE internal_filemeta CASCADE;
CREATE TABLE public.filemeta (
    dirhash int8 NOT NULL,
    "name" TEXT NOT NULL,
    directory TEXT NULL,
    meta bytea NULL,
    json_meta NULL,
    CONSTRAINT filemeta_pkey PRIMARY KEY (dirhash, name)
);

CREATE OR REPLACE FUNCTION create_new_table (name TEXT)
    RETURNS void
    LANGUAGE plpgsql
    AS $f$
DECLARE 
str TEXT;
BEGIN
    SELECT format(
        $t$
        CREATE TABLE %1$I (
            dirhash BIGINT NOT NULL,
            "name" TEXT NOT NULL,
            directory TEXT NULL,
            meta bytea NULL,
            json_meta jsonb NULL,
            CONSTRAINT %1$I_pkey PRIMARY KEY (dirhash, name)
        );
      	CREATE INDEX %1$I_name_trgm_idx ON %1$I USING gin (name gin_trgm_ops);
	    CREATE INDEX %1$I_directory_trgm_idx ON %1$I USING gin (directory gin_trgm_ops);
--	   	CREATE INDEX %1$I_isdirectory_idx ON %1$I (isdirectory);
		CREATE INDEX %1$I_json_meta_gin_idx ON %1$I USING gin(json_meta);
      	$t$
        , name
    ) INTO str;
   
   RAISE NOTICE '%', str;
  EXECUTE format('%s', str);
END;
$f$;


CREATE OR REPLACE VIEW public.filemeta AS
SELECT
    internal_filemeta.dirhash,
    internal_filemeta.name,
    internal_filemeta.directory,
    internal_filemeta.meta,
    NULL::text AS json_meta
FROM
    internal_filemeta;

CREATE OR REPLACE VIEW public.size_by_directory_png
AS SELECT f.dirhash,
    f.directory,
    sum(f.size) AS size
   FROM internal_json_meta f
  WHERE f.name::text ~~ '%.png'::text
  GROUP BY f.dirhash, f.directory
  ORDER BY (sum(f.size)) DESC;

  
CREATE TABLE system_meta_counts (
    metric text , collection text, counts bigint,
    CONSTRAINT system_meta_counts_pk PRIMARY KEY (metric, collection)
);

CREATE TABLE system_meta_size (
    metric text , collection text, size bigint,
    CONSTRAINT system_meta_counts_pk PRIMARY KEY (metric, collection)
);