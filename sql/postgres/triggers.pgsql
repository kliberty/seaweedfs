
BEGIN;
-- CREATE TRIGGER filemeta_insert_trigger
--     INSTEAD OF INSERT ON filemeta FOR EACH ROW
--     EXECUTE FUNCTION fmeta_pgsql_ins ();

-- CREATE TRIGGER filemeta_delete_trigger
--     INSTEAD OF DELETE ON filemeta FOR EACH ROW
--     EXECUTE FUNCTION fmeta_pgsql_del ();

INSERT INTO system_meta_counts SELECT 'directories', count(*) FROM internal_json_meta ijm WHERE (json_meta ->> 'IsDirectory')::bool = TRUE;
INSERT INTO system_meta_counts SELECT 'files', count(*) FROM internal_json_meta ijm WHERE (json_meta ->> 'IsDirectory')::bool = FALSE;
CREATE TRIGGER json_insert_trigger
    AFTER INSERT OR DELETE ON internal_json_meta FOR EACH ROW
    EXECUTE FUNCTION update_counts ();
END;
