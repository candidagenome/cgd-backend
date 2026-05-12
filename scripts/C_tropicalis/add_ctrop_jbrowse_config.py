#!/usr/bin/env python3
"""
Add C. tropicalis protein assembly and domain tracks to JBrowse2 config.

Usage:
    python add_ctrop_jbrowse_config.py

Run on the frontend server where /data/jbrowse2/config.json exists.
"""
import json

config_file = '/data/jbrowse2/config.json'

with open(config_file, 'r') as f:
    config = json.load(f)

new_assembly = {
    "name": "C_tropicalis_prot",
    "sequence": {
        "type": "ReferenceSequenceTrack",
        "trackId": "C_tropicalis_prot-ReferenceSequenceTrack",
        "adapter": {
            "type": "IndexedFastaAdapter",
            "fastaLocation": {
                "uri": "protein_data/C_tropicalis_proteins.fasta",
                "locationType": "UriLocation"
            },
            "faiLocation": {
                "uri": "protein_data/C_tropicalis_proteins.fasta.fai",
                "locationType": "UriLocation"
            }
        }
    }
}
config['assemblies'].append(new_assembly)

databases = ['Pfam', 'PANTHER', 'SUPERFAMILY', 'CATH', 'SMART', 'ProSiteProfiles', 'CDD', 'PRINTS', 'Coils', 'MobiDBLite']
colors = {'Pfam': 'orange', 'PANTHER': 'blue', 'SUPERFAMILY': 'green', 'CATH': 'purple',
          'SMART': 'red', 'ProSiteProfiles': 'teal', 'CDD': 'brown', 'PRINTS': 'pink',
          'Coils': 'gray', 'MobiDBLite': 'cyan'}

for db in databases:
    track = {
        "type": "FeatureTrack",
        "trackId": f"C_tropicalis_{db}",
        "name": db,
        "adapter": {
            "type": "Gff3TabixAdapter",
            "gffGzLocation": {
                "uri": f"protein_data/C_tropicalis_{db}.gff.gz",
                "locationType": "UriLocation"
            },
            "index": {
                "location": {
                    "uri": f"protein_data/C_tropicalis_{db}.gff.gz.tbi",
                    "locationType": "UriLocation"
                },
                "indexType": "TBI"
            }
        },
        "assemblyNames": ["C_tropicalis_prot"],
        "category": ["Protein Domains"],
        "displays": [{
            "type": "LinearBasicDisplay",
            "displayId": f"C_tropicalis_{db}-LinearBasicDisplay",
            "renderer": {
                "type": "SvgFeatureRenderer",
                "color1": colors.get(db, 'orange')
            }
        }]
    }
    config['tracks'].append(track)

with open(config_file, 'w') as f:
    json.dump(config, f, indent=2)

print(f"Added assembly and {len(databases)} tracks for C_tropicalis")
