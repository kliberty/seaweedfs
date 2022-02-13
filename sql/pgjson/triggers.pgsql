
BEGIN;

CREATE TRIGGER filemeta_counts_trigger
    AFTER INSERT OR DELETE ON filemeta FOR EACH ROW
    EXECUTE FUNCTION update_counts ();
    
END;
