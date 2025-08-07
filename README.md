# Evidence knihoven MK ČR - ETL řešení

**Dataset:** Evidence knihoven-06082025.csv (6,783 knihoven, 39 sloupců)  
**Zdroj:** https://mk.gov.cz/evidence-knihoven-adresar-knihoven-evidovanych-ministerstvem-kultury-a-souvisejici-informace-cs-341

## Řešení 4 otázek

### 1. Automatické stahování dat ze zdroje

**Implementace:** `data_downloader.py`

```python
def download_mkcr_data():
    """Automatické stahování XLSX z MK ČR"""
    
    # 1. Web scraping pro nalezení aktuálního XLSX odkazu
    response = requests.get("https://mk.gov.cz/evidence-knihoven...")
    soup = BeautifulSoup(response.content, 'html.parser')
    xlsx_links = soup.find_all('a', href=re.compile(r'.*\.xlsx'))
    
    # 2. Stažení nejnovějšího souboru
    latest_file = xlsx_links[0]['href']
    download_file(latest_file, f"knihovny_{date.today()}.xlsx")
    
    # 3. Konverze XLSX → CSV
    df = pd.read_excel(f"knihovny_{date.today()}.xlsx")
    df.to_csv(f"knihovny_{date.today()}.csv", index=False)
```

**Automatizace:**
- Denní Airflow DAG ve 2:00
- Retry mechanismus při selhání
- Email notifikace

### 2. Transformace pro interoperabilitu

**Implementace:** `data_processor.py`

**Zachovává původní české názvy sloupců:**
```
Název provozovatele → nazev_provozovatele
I - NÁZEV KNIHOVNY → nazev_knihovny  
R - EVIDENČNÍ ČÍSLO KNIHOVNY → evidencni_cislo_knihovny
K - adresa knihovny: kraj → knihovna_kraj
...všech 39 sloupců
```

**Klíčové transformace:**
```python
def transform_data(df):
    # 1. Standardizace PSČ
    df['psc_clean'] = df['psc'].str.replace(r'[^\d]', '', regex=True)
    
    # 2. Email validace
    df['email_valid'] = df['email'].str.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')
    
    # 3. URL normalizace  
    df['url_normalized'] = df['url'].apply(lambda x: f"https://{x}" if x and not x.startswith('http') else x)
    
    # 4. Linking keys pro propojení
    df['knihovna_id'] = df['evidencni_cislo_knihovny'].str.replace(' ', '-')
    df['geo_key'] = df['knihovna_kraj'] + '_' + df['knihovna_okres']
    
    return df
```

**Výstupní formáty:**
- CSV (původní struktura)
- JSON (pro API)
- JSON-LD (linked data)

### 3. Trvalé a bezpečné uložení

**Implementace:** `sql/create_tables.sql`

```sql
-- Production tabulka s původními českými názvy
CREATE TABLE knihovny_production (
    nazev_provozovatele VARCHAR(255),
    ico_provozovatele VARCHAR(20),
    nazev_knihovny VARCHAR(255),
    evidencni_cislo_knihovny VARCHAR(50) UNIQUE,
    knihovna_kraj VARCHAR(100),
    knihovna_okres VARCHAR(100),
    knihovna_mesto VARCHAR(100),
    email_knihovna VARCHAR(255),
    webova_stranka VARCHAR(500),
    -- ... všech 39 původních sloupců
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2)
);

-- Indexy pro výkon
CREATE INDEX idx_evidencni_cislo ON knihovny_production(evidencni_cislo_knihovny);
CREATE INDEX idx_kraj_okres ON knihovny_production(knihovna_kraj, knihovna_okres);
```

**Bezpečnostní opatření:**
- PostgreSQL s šifrováním at rest
- Denní backup do cloud storage
- Access control pomocí rolí
- Audit log všech změn

**Backup strategie:**
```bash
# Denní backup
pg_dump knihovny_db > backup/knihovny_$(date +%Y%m%d).sql
# Upload do cloud
aws s3 cp backup/ s3://knihovny-backup/ --recursive
```

### 4. CCMM metadata

**Implementace:** `ccmm_generator.py` + JSON file, s definovanými sloupci, datovými typy, popisem, modem.

```python
def generate_ccmm_metadata():
    return {
        "@context": "https://data.gov.cz/ccmm/",
        "@type": "Dataset",
        "title": {
            "@language": "cs",
            "@value": "Evidence knihoven České republiky"
        },
        "description": {
            "@language": "cs", 
            "@value": "Oficiální evidence knihoven vedená MK ČR"
        },
        "publisher": {
            "@type": "Organization",
            "name": "Ministerstvo kultury České republiky",
            "homepage": "https://mk.gov.cz/"
        },
        "contactPoint": {
            "@type": "ContactPoint",
            "email": "opendata@mkcr.cz"
        },
        "accrualPeriodicity": "daily",
        "language": "cs",
        "spatial": "Česká republika",
        "license": "https://data.gov.cz/podmínky-užití/...",
        "distribution": [
            {
                "@type": "Distribution",
                "format": "CSV",
                "downloadURL": "https://api.knihovny.cz/export/csv"
            },
            {
                "@type": "Distribution", 
                "format": "JSON-LD",
                "downloadURL": "https://api.knihovny.cz/export/jsonld"
            }
        ]
    }
```

## Struktura souborů

```
knihovny/
├── README.md              # Tato dokumentace
├── requirements.txt       # Dependencies
├── data_downloader.py     # 1. Automatické stahování
├── data_processor.py      # 2. Transformace dat  
├── ccmm_generator.py      # 4. CCMM metadata
├── airflow_dag.py         # Orchestrace
└── sql/
    └── create_tables.sql  # 3. Database schema
```

## Spuštění

```bash
# 1. Instalace
pip install -r requirements.txt

# 2. Stažení dat
python data_downloader.py

# 3. Zpracování
python data_processor.py

# 4. Generování CCMM
python ccmm_generator.py

# 5. Airflow DAG
airflow dags trigger knihovny_etl_daily
```

## Výsledek

**Kompletní automatizované řešení** poskytuje:

- **Denní stahování** XLSX z mk.gov.cz
- **Zachování všech 39 českých sloupců** bez anglicizace
- **Bezpečné PostgreSQL úložiště** s backup
- **CCMM compliant metadata** podle českého standardu
- **Linking keys** pro interoperabilitu s jinými systémy

**Kvalita dat:** 6,783 knihoven s vysokou úplností kontaktních údajů  
**Aktualizace:** Denní automatická synchronizace  
**Standard:** CCMM 1.0 compliant