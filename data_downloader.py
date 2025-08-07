#!/usr/bin/env python3
"""
Automatické stahování XLSX dat z MK ČR
Řešení pro otázku 1: Automatické stahování dat ze zdroje
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import date, datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class MKCRDataDownloader:
    """Downloader pro automatické stahování knihoven z MK ČR"""
    
    def __init__(self):
        self.base_url = "https://mk.gov.cz"
        self.evidence_page = "/evidence-knihoven-adresar-knihoven-evidovanych-ministerstvem-kultury-a-souvisejici-informace-cs-341"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def find_xlsx_links(self):
        """Najde XLSX odkazy na stránce MK ČR"""
        try:
            url = f"{self.base_url}{self.evidence_page}"
            response = self.session.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Hledání XLSX odkazů
            xlsx_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '.xlsx' in href.lower() and 'evidence' in href.lower():
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    xlsx_links.append({
                        'url': full_url,
                        'text': link.get_text(strip=True),
                        'found_at': datetime.now()
                    })
            
            logger.info(f"Found {len(xlsx_links)} XLSX links")
            return xlsx_links
            
        except Exception as e:
            logger.error(f"Error finding XLSX links: {e}")
            return []
    
    def download_file(self, url, output_path):
        """Stáhne soubor z URL"""
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"File downloaded: {output_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            raise
    
    def convert_xlsx_to_csv(self, xlsx_path, csv_path):
        """Konvertuje XLSX na CSV"""
        try:
            df = pd.read_excel(xlsx_path)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Converted to CSV: {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"Error converting XLSX to CSV: {e}")
            raise
    
    def download_latest_evidence(self, output_dir="data"):
        """
        Stáhne nejnovější evidenci knihoven
        
        Returns:
            Dict s informacemi o stažených souborech
        """
        try:
            # Najdeme XLSX odkazy
            xlsx_links = self.find_xlsx_links()
            if not xlsx_links:
                raise ValueError("No XLSX links found")
            
            # Vybereme první nalezený
            selected_link = xlsx_links[0]
            
            # Názvy výstupních souborů
            today = date.today().strftime("%Y%m%d")
            xlsx_file = f"{output_dir}/knihovny_evidence_{today}.xlsx"
            csv_file = f"{output_dir}/knihovny_evidence_{today}.csv"
            
            # Stáhneme XLSX
            self.download_file(selected_link['url'], xlsx_file)
            
            # Konvertujeme na CSV
            self.convert_xlsx_to_csv(xlsx_file, csv_file)
            
            return {
                'status': 'success',
                'xlsx_file': xlsx_file,
                'csv_file': csv_file,
                'source_url': selected_link['url'],
                'download_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'download_time': datetime.now().isoformat()
            }

def main():
    """Test funkce"""
    logging.basicConfig(level=logging.INFO)
    
    downloader = MKCRDataDownloader()
    result = downloader.download_latest_evidence()
    
    if result['status'] == 'success':
        print(f"SUCCESS: Downloaded {result['csv_file']}")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()