-- SQL schema pro trvalé a bezpečné uložení dat
-- Řešení pro otázku 3: Trvalé a bezpečné uložení dat

-- Hlavní tabulka knihoven (zachovává původní české názvy sloupců)
CREATE TABLE knihovny_production (
    -- Primární identifikace
    evidencni_cislo_knihovny VARCHAR(50) PRIMARY KEY,
    nazev_knihovny VARCHAR(255) NOT NULL,
    
    -- Provozovatel (všechny původní české sloupce)
    nazev_provozovatele VARCHAR(255),
    ico_provozovatele VARCHAR(20),
    sidlo_ulice VARCHAR(255),
    sidlo_psc VARCHAR(10),
    sidlo_obec VARCHAR(100),
    sidlo_okres VARCHAR(100),
    sidlo_kraj VARCHAR(100),
    datova_schranka VARCHAR(50),
    predmet_cinnosti TEXT,
    pravni_forma VARCHAR(100),
    datum_narozeni_provozovatele DATE,
    
    -- Knihovna údaje
    druh_knihovny VARCHAR(100),
    kategorie VARCHAR(100),
    knihovna_ulice VARCHAR(255),
    knihovna_psc VARCHAR(10),
    knihovna_mesto VARCHAR(100),
    knihovna_okres VARCHAR(100),
    knihovna_kraj VARCHAR(100),
    
    -- Kontaktní údaje
    email_autor VARCHAR(255),
    email_knihovna VARCHAR(255),
    webova_stranka VARCHAR(500),
    provozni_doba TEXT,
    dalsi_informace TEXT,
    
    -- Metadata
    sigla VARCHAR(50),
    datum_vytvoreni DATE,
    datum_evidence DATE,
    schvalovatel_zaznamu VARCHAR(100),
    datum_aktualizace DATE,
    schvalovatel_aktualizace VARCHAR(100),
    
    -- Status
    aktivni_zrusena VARCHAR(20),
    is_active BOOLEAN,
    datum_vyrazeni DATE,
    vyradil_z_evidence VARCHAR(100),
    duvod_vyrazeni TEXT,
    cislo_jednaci VARCHAR(50),
    poznamka TEXT,
    
    -- Transformované hodnoty pro interoperabilitu
    psc_normalized VARCHAR(10),
    email_valid BOOLEAN,
    webova_stranka_normalized VARCHAR(500),
    knihovna_id VARCHAR(100),
    knihovna_hash VARCHAR(32),
    geo_key VARCHAR(200),
    library_uri VARCHAR(500),
    
    -- System metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2),
    last_sync_date DATE
);

-- Indexy pro výkon
CREATE INDEX idx_knihovny_evidencni_cislo ON knihovny_production(evidencni_cislo_knihovny);
CREATE INDEX idx_knihovny_kraj_okres ON knihovny_production(knihovna_kraj, knihovna_okres);
CREATE INDEX idx_knihovny_active ON knihovny_production(is_active);
CREATE INDEX idx_knihovny_sync_date ON knihovny_production(last_sync_date);
CREATE INDEX idx_knihovny_quality ON knihovny_production(data_quality_score);

-- Tabulka pro sledování změn (audit log)
CREATE TABLE knihovny_audit (
    audit_id SERIAL PRIMARY KEY,
    evidencni_cislo_knihovny VARCHAR(50) NOT NULL,
    change_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    changed_fields JSONB,
    old_values JSONB,
    new_values JSONB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'etl_system',
    dag_run_id VARCHAR(100)
);

CREATE INDEX idx_audit_knihovna ON knihovny_audit(evidencni_cislo_knihovny);
CREATE INDEX idx_audit_date ON knihovny_audit(changed_at);

-- Tabulka pro quality metriky
CREATE TABLE data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    total_records INTEGER,
    active_libraries INTEGER,
    email_completeness DECIMAL(4,3),
    web_completeness DECIMAL(4,3),
    overall_quality_score DECIMAL(4,3),
    unique_regions INTEGER,
    processing_duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_date ON data_quality_metrics(metric_date);

-- Tabulka pro CCMM metadata
CREATE TABLE ccmm_metadata (
    metadata_id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(100) NOT NULL,
    ccmm_version VARCHAR(10) DEFAULT '1.0',
    metadata_json JSONB NOT NULL,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Trigger pro automatické audit log
CREATE OR REPLACE FUNCTION knihovny_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO knihovny_audit (
            evidencni_cislo_knihovny, change_type, new_values
        ) VALUES (
            NEW.evidencni_cislo_knihovny, 'INSERT', row_to_json(NEW)
        );
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO knihovny_audit (
            evidencni_cislo_knihovny, change_type, old_values, new_values
        ) VALUES (
            NEW.evidencni_cislo_knihovny, 'UPDATE', row_to_json(OLD), row_to_json(NEW)
        );
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO knihovny_audit (
            evidencni_cislo_knihovny, change_type, old_values
        ) VALUES (
            OLD.evidencni_cislo_knihovny, 'DELETE', row_to_json(OLD)
        );
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Aplikace triggeru
CREATE TRIGGER knihovny_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON knihovny_production
    FOR EACH ROW EXECUTE FUNCTION knihovny_audit_trigger();

-- View pro aktuální aktivní knihovny
CREATE VIEW knihovny_active AS
SELECT 
    evidencni_cislo_knihovny,
    nazev_knihovny,
    knihovna_kraj,
    knihovna_okres,
    knihovna_mesto,
    email_knihovna,
    webova_stranka_normalized,
    data_quality_score
FROM knihovny_production 
WHERE is_active = TRUE
ORDER BY knihovna_kraj, knihovna_okres, nazev_knihovny;

-- View pro statistiky podle krajů
CREATE VIEW knihovny_stats_by_region AS
SELECT 
    knihovna_kraj,
    COUNT(*) as total_libraries,
    COUNT(CASE WHEN is_active THEN 1 END) as active_libraries,
    COUNT(CASE WHEN email_valid THEN 1 END) as libraries_with_email,
    AVG(data_quality_score) as avg_quality_score
FROM knihovny_production
GROUP BY knihovna_kraj
ORDER BY total_libraries DESC;

-- Bezpečnostní role
CREATE ROLE knihovny_reader;
GRANT SELECT ON knihovny_production, knihovny_active, knihovny_stats_by_region TO knihovny_reader;
GRANT SELECT ON data_quality_metrics, ccmm_metadata TO knihovny_reader;

CREATE ROLE knihovny_writer;
GRANT knihovny_reader TO knihovny_writer;
GRANT INSERT, UPDATE ON knihovny_production TO knihovny_writer;
GRANT INSERT ON knihovny_audit, data_quality_metrics, ccmm_metadata TO knihovny_writer;

-- Komentáře k tabulkám
COMMENT ON TABLE knihovny_production IS 'Hlavní tabulka evidence knihoven MK ČR - zachovává původní české názvy sloupců';
COMMENT ON TABLE knihovny_audit IS 'Audit log všech změn v evidenci knihoven';
COMMENT ON TABLE data_quality_metrics IS 'Denní metriky kvality dat';
COMMENT ON TABLE ccmm_metadata IS 'CCMM metadata pro dataset knihoven';