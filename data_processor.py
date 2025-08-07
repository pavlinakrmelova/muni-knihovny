#!/usr/bin/env python3
"""
Zpracování a transformace dat knihoven
Řešení pro otázku 2: Transformace a úpravy pro interoperabilitu
"""

import pandas as pd
import re
import json
import hashlib
from datetime import datetime
from pathlib import Path

class KnihovnyDataProcessor:
    """Procesor pro transformaci dat knihoven"""
    
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
        
    def load_data(self):
        """Načte CSV data s původními českými názvy sloupců"""
        try:
            self.df = pd.read_csv(
                self.csv_file_path,
                encoding='utf-8',
                dtype=str,
                na_values=['', 'NULL', 'null', 'N/A'],
                keep_default_na=False
            )
            print(f"Loaded {len(self.df)} records with {len(self.df.columns)} columns")
            return self.df
            
        except Exception as e:
            print(f"Error loading CSV: {e}")
            raise
    
    def transform_data(self):
        """
        Transformuje data pro interoperabilitu
        Zachovává původní české názvy sloupců
        """
        if self.df is None:
            self.load_data()
        
        # Kopie pro transformace
        df_transformed = self.df.copy()
        
        # 1. PSČ normalizace
        psc_columns = [col for col in df_transformed.columns if 'PSČ' in col]
        for col in psc_columns:
            df_transformed[f'{col}_clean'] = df_transformed[col].str.replace(r'[^\d]', '', regex=True)
        
        # 2. Email validace
        email_columns = [col for col in df_transformed.columns if 'e-mail' in col.lower()]
        for col in email_columns:
            df_transformed[f'{col}_valid'] = df_transformed[col].str.match(
                r'^[^\s@]+@[^\s@]+\.[^\s@]+$', na=False
            )
        
        # 3. URL normalizace
        url_columns = [col for col in df_transformed.columns if 'webov' in col.lower()]
        for col in url_columns:
            df_transformed[f'{col}_normalized'] = df_transformed[col].apply(
                lambda x: self._normalize_url(x)
            )
        
        # 4. Status normalizace
        if 'aktivní / zrušená (vyřazená z evidence)' in df_transformed.columns:
            df_transformed['is_active'] = df_transformed['aktivní / zrušená (vyřazená z evidence)'].apply(
                lambda x: str(x).lower().strip() == 'aktivní' if pd.notna(x) else False
            )
        
        # 5. Linking keys pro interoperabilitu
        self._add_linking_keys(df_transformed)
        
        self.df_transformed = df_transformed
        return df_transformed
    
    def _normalize_url(self, url):
        """Normalizuje URL adresy"""
        if pd.isna(url) or url == '':
            return None
        
        url = str(url).strip()
        if not url.startswith(('http://', 'https://')):
            if url.startswith('www.'):
                url = 'https://' + url
            else:
                url = 'https://www.' + url
        return url
    
    def _add_linking_keys(self, df):
        """Přidá propojovací klíče pro interoperabilitu"""
        
        # Knihovna hash (pro jedinečnou identifikaci)
        if 'R - EVIDENČNÍ ČÍSLO KNIHOVNY' in df.columns:
            df['knihovna_id'] = df['R - EVIDENČNÍ ČÍSLO KNIHOVNY'].str.replace(' ', '-', regex=False)
            df['knihovna_hash'] = df['R - EVIDENČNÍ ČÍSLO KNIHOVNY'].apply(
                lambda x: hashlib.md5(str(x).encode()).hexdigest() if pd.notna(x) else None
            )
        
        # Geografický klíč
        if all(col in df.columns for col in ['K - adresa knihovny: kraj', 'K - adresa knihovny: okres']):
            df['geo_key'] = (
                df['K - adresa knihovny: kraj'].astype(str) + '_' + 
                df['K - adresa knihovny: okres'].astype(str)
            )
        
        # URI pro linked data
        if 'R - EVIDENČNÍ ČÍSLO KNIHOVNY' in df.columns:
            df['library_uri'] = df['R - EVIDENČNÍ ČÍSLO KNIHOVNY'].apply(
                lambda x: f"https://knihovny.cz/library/{str(x).replace(' ', '-')}" if pd.notna(x) else None
            )
    
    def calculate_quality_metrics(self):
        """Vypočítá metriky kvality dat"""
        if not hasattr(self, 'df_transformed'):
            self.transform_data()
        
        df = self.df_transformed
        total_records = len(df)
        
        # Email completeness
        email_col = next((col for col in df.columns if 'e-mail' in col.lower() and 'knihovn' in col.lower()), None)
        email_completeness = df[email_col].notna().sum() / total_records if email_col else 0
        
        # Web completeness  
        web_col = next((col for col in df.columns if 'webov' in col.lower()), None)
        web_completeness = df[web_col].notna().sum() / total_records if web_col else 0
        
        # Active libraries
        active_ratio = df['is_active'].sum() / total_records if 'is_active' in df.columns else 0
        
        return {
            'total_records': total_records,
            'email_completeness': email_completeness,
            'web_completeness': web_completeness, 
            'active_ratio': active_ratio,
            'quality_score': (email_completeness + web_completeness + active_ratio) / 3,
            'calculated_at': datetime.now().isoformat()
        }
    
    def export_formats(self, output_dir="processed_data"):
        """Exportuje data do různých formátů pro interoperabilitu"""
        if not hasattr(self, 'df_transformed'):
            self.transform_data()
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        exported_files = {}
        
        # 1. CSV (původní struktura s transformacemi)
        csv_file = output_path / f"knihovny_processed_{timestamp}.csv"
        self.df_transformed.to_csv(csv_file, index=False, encoding='utf-8')
        exported_files['csv'] = str(csv_file)
        
        # 2. JSON pro API
        json_file = output_path / f"knihovny_api_{timestamp}.json"
        json_data = self.df_transformed.to_dict('records')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
        exported_files['json'] = str(json_file)
        
        # 3. JSON-LD pro linked data
        jsonld_file = output_path / f"knihovny_linked_{timestamp}.jsonld"
        self._export_jsonld(jsonld_file)
        exported_files['jsonld'] = str(jsonld_file)
        
        print(f"Data exported to {len(exported_files)} formats")
        return exported_files
    
    def _export_jsonld(self, output_file):
        """Exportuje data jako JSON-LD pro linked data"""
        if not hasattr(self, 'df_transformed'):
            return
        
        libraries = []
        for _, row in self.df_transformed.iterrows():
            library = {
                "@context": "https://schema.org/",
                "@type": "Library",
                "@id": row.get('library_uri'),
                "name": row.get('I - NÁZEV KNIHOVNY'),
                "identifier": row.get('R - EVIDENČNÍ ČÍSLO KNIHOVNY'),
                "address": {
                    "@type": "PostalAddress",
                    "streetAddress": row.get('K - adresa knihovny: ulice'),
                    "postalCode": row.get('K - adresa knihovny: PSČ'),
                    "addressLocality": row.get('K - adresa knihovny: město'),
                    "addressRegion": row.get('K - adresa knihovny: kraj'),
                    "addressCountry": "CZ"
                },
                "email": row.get('N - e-mailový kontakt na knihovnu'),
                "url": row.get('O - odkaz na webovou stránku knihovny, respektive odkaz na informace o knihovně na webových stránkách provozovatele_normalized')
            }
            
            # Odstranění None hodnot
            library = {k: v for k, v in library.items() if v is not None and v != ''}
            libraries.append(library)
        
        jsonld_data = {
            "@context": "https://schema.org/",
            "@graph": libraries
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(jsonld_data, f, ensure_ascii=False, indent=2)

def main():
    """Test funkce"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python data_processor.py <csv_file>")
        return
    
    processor = KnihovnyDataProcessor(sys.argv[1])
    
    # Kompletní pipeline
    processor.load_data()
    processor.transform_data()
    metrics = processor.calculate_quality_metrics()
    files = processor.export_formats()
    
    print(f"Processing completed:")
    print(f"Quality score: {metrics['quality_score']:.2%}")
    print(f"Exported files: {list(files.keys())}")

if __name__ == "__main__":
    main()