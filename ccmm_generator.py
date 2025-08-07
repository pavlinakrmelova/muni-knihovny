#!/usr/bin/env python3
"""
CCMM (Czech Core Metadata Model) generátor
Řešení pro otázku 4: Popis datové sady pomocí CCMM
"""

import json
from datetime import datetime
from pathlib import Path

class CCMMGenerator:
    """Generátor CCMM metadat pro evidenci knihoven"""
    
    def __init__(self):
        self.base_uri = "https://knihovny.cz/"
        
    def generate_dataset_metadata(self, stats=None):
        """
        Generuje CCMM metadata pro dataset knihoven
        
        Args:
            stats: Statistiky zpracování dat (volitelné)
            
        Returns:
            CCMM metadata jako dictionary
        """
        
        if stats is None:
            stats = {}
        
        ccmm_metadata = {
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "vcard": "http://www.w3.org/2006/vcard/ns#",
                "ccmm": "https://data.gov.cz/ccmm/"
            },
            "@type": "dcat:Dataset",
            "@id": f"{self.base_uri}dataset/knihovny-evidence",
            
            # Základní informace
            "dct:title": {
                "@language": "cs",
                "@value": "Evidence knihoven České republiky"
            },
            "dct:description": {
                "@language": "cs", 
                "@value": "Oficiální evidence knihoven vedená Ministerstvem kultury České republiky podle zákona č. 257/2001 Sb."
            },
            
            # Publisher
            "dct:publisher": {
                "@type": "foaf:Organization",
                "foaf:name": {
                    "@language": "cs",
                    "@value": "Ministerstvo kultury České republiky"
                },
                "foaf:homepage": "https://mk.gov.cz/"
            },
            
            # Kontakt
            "dcat:contactPoint": {
                "@type": "vcard:Organization",
                "vcard:fn": "Oddělení otevřených dat MK ČR",
                "vcard:hasEmail": "mailto:opendata@mkcr.cz"
            },
            
            # Spatial coverage
            "dct:spatial": {
                "@type": "dct:Location",
                "skos:prefLabel": {
                    "@language": "cs",
                    "@value": "Česká republika"
                }
            },
            
            # Temporal
            "dct:accrualPeriodicity": "http://publications.europa.eu/resource/authority/frequency/DAILY",
            
            # Language
            "dct:language": "http://publications.europa.eu/resource/authority/language/CES",
            
            # License
            "dct:license": "https://data.gov.cz/podmínky-užití/obsahem-chráněné-databáze-autorským-právem/",
            
            # Keywords
            "dcat:keyword": [
                {"@language": "cs", "@value": "knihovny"},
                {"@language": "cs", "@value": "kultura"},
                {"@language": "cs", "@value": "veřejné služby"},
                {"@language": "cs", "@value": "evidence"},
                {"@language": "en", "@value": "libraries"},
                {"@language": "en", "@value": "culture"}
            ],
            
            # Themes
            "dcat:theme": [
                "http://publications.europa.eu/resource/authority/data-theme/EDUC",
                "http://publications.europa.eu/resource/authority/data-theme/GOVE"
            ],
            
            # Dates
            "dct:issued": {
                "@type": "xsd:date",
                "@value": "2001-01-01"  # Zákon o knihovnách
            },
            "dct:modified": {
                "@type": "xsd:dateTime",
                "@value": datetime.now().isoformat()
            },
            
            # Distributions
            "dcat:distribution": self._generate_distributions()
        }
        
        # Přidání quality measurements pokud máme statistiky
        if stats:
            ccmm_metadata["dqv:hasQualityMeasurement"] = self._generate_quality_measurements(stats)
        
        return ccmm_metadata
    
    def _generate_distributions(self):
        """Generuje dostupné distribuce dat"""
        
        distributions = []
        
        # CSV distribuce
        distributions.append({
            "@type": "dcat:Distribution",
            "@id": f"{self.base_uri}distribution/knihovny-csv",
            "dct:title": {
                "@language": "cs",
                "@value": "Evidence knihoven - CSV"
            },
            "dct:format": "http://publications.europa.eu/resource/authority/file-type/CSV",
            "dcat:mediaType": "text/csv",
            "dcat:downloadURL": f"{self.base_uri}api/export/csv"
        })
        
        # JSON-LD distribuce
        distributions.append({
            "@type": "dcat:Distribution",
            "@id": f"{self.base_uri}distribution/knihovny-jsonld", 
            "dct:title": {
                "@language": "cs",
                "@value": "Evidence knihoven - JSON-LD"
            },
            "dct:format": "http://publications.europa.eu/resource/authority/file-type/JSON_LD",
            "dcat:mediaType": "application/ld+json",
            "dcat:downloadURL": f"{self.base_uri}api/export/jsonld"
        })
        
        return distributions
    
    def _generate_quality_measurements(self, stats):
        """Generuje quality measurements z dat"""
        
        measurements = []
        
        # Completeness
        if 'email_completeness' in stats:
            measurements.append({
                "@type": "dqv:QualityMeasurement",
                "dqv:isMeasurementOf": {
                    "@type": "dqv:Metric",
                    "skos:prefLabel": {
                        "@language": "cs",
                        "@value": "Úplnost emailových kontaktů"
                    }
                },
                "dqv:value": round(stats['email_completeness'], 3)
            })
        
        # Overall quality
        if 'quality_score' in stats:
            measurements.append({
                "@type": "dqv:QualityMeasurement", 
                "dqv:isMeasurementOf": {
                    "@type": "dqv:Metric",
                    "skos:prefLabel": {
                        "@language": "cs",
                        "@value": "Celkové skóre kvality dat"
                    }
                },
                "dqv:value": round(stats['quality_score'], 3)
            })
        
        return measurements
    
    def validate_ccmm(self, metadata):
        """Základní validace CCMM souladu"""
        
        required_fields = [
            '@context', '@type', 'dct:title', 'dct:description',
            'dct:publisher', 'dcat:contactPoint'
        ]
        
        validation = {
            'is_valid': True,
            'missing_fields': [],
            'warnings': []
        }
        
        # Kontrola povinných polí
        for field in required_fields:
            if field not in metadata:
                validation['missing_fields'].append(field)
                validation['is_valid'] = False
        
        # Kontrola distribucí
        if 'dcat:distribution' not in metadata:
            validation['warnings'].append('No distributions defined')
        
        return validation
    
    def export_metadata(self, metadata, output_path="ccmm_metadata.json"):
        """Exportuje CCMM metadata do souboru"""
        
        output_file = Path(output_path)
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        print(f"CCMM metadata exported to {output_file}")
        return str(output_file)

def main():
    """Test funkce"""
    
    # Ukázková statistika
    test_stats = {
        'total_records': 6783,
        'email_completeness': 0.67,
        'web_completeness': 0.45,
        'quality_score': 0.78
    }
    
    # Generování metadat
    generator = CCMMGenerator()
    metadata = generator.generate_dataset_metadata(test_stats)
    
    # Validace
    validation = generator.validate_ccmm(metadata)
    print(f"CCMM validation: {'PASSED' if validation['is_valid'] else 'FAILED'}")
    
    if validation['missing_fields']:
        print(f"Missing fields: {validation['missing_fields']}")
    if validation['warnings']:
        print(f"Warnings: {validation['warnings']}")
    
    # Export
    output_file = generator.export_metadata(metadata)
    print(f"CCMM metadata generated: {output_file}")

if __name__ == "__main__":
    main()