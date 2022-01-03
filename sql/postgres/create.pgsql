CREATE EXTENSION pg_trgm;

-- DROP TABLE internal_filemeta CASCADE;
CREATE TABLE public.internal_filemeta (
    dirhash int8 NOT NULL,
    "name" varchar(65535) NOT NULL,
    directory varchar(65535) NULL,
    meta bytea NULL,
    isdirectory bool NULL,
    filesize bigint NULL,
    CONSTRAINT filemeta_pkey PRIMARY KEY (dirhash, name)
);

CREATE INDEX filemeta_name_trgm_idx ON internal_filemeta USING gin (name gin_trgm_ops);

CREATE INDEX filemeta_directory_trgm_idx ON internal_filemeta USING gin (directory gin_trgm_ops);

CREATE INDEX filemeta_isdirectory_idx ON internal_filemeta (isdirectory);

-- DROP TABLE internal_json_meta CASCADE;
CREATE TABLE internal_json_meta (
    dirhash int8 NOT NULL,
    name text NOT NULL,
    json_meta jsonb
);

ALTER TABLE public.internal_json_meta
    ADD CONSTRAINT internal_json_meta_fk FOREIGN KEY (dirhash, "name") REFERENCES public.internal_filemeta (dirhash, "name") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE UNIQUE INDEX internal_json_fk_idx ON internal_json_meta (dirhash, name);

CREATE OR REPLACE VIEW public.filemeta AS
SELECT
    internal_filemeta.dirhash,
    internal_filemeta.name,
    internal_filemeta.directory,
    internal_filemeta.meta,
    NULL::text AS json_meta
FROM
    internal_filemeta;
