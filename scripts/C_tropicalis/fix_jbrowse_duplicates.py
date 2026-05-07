#!/usr/bin/env python3
"""
Fix duplicate assemblies and tracks in JBrowse2 config.

Usage:
    python fix_jbrowse_duplicates.py

Run on the frontend server where /data/jbrowse2/config.json exists.
"""
import json

config_file = '/data/jbrowse2/config.json'

with open(config_file, 'r') as f:
    config = json.load(f)

# Remove duplicate assemblies
seen = set()
unique_assemblies = []
for assembly in config['assemblies']:
    name = assembly.get('name')
    if name not in seen:
        seen.add(name)
        unique_assemblies.append(assembly)
    else:
        print(f"Removing duplicate assembly: {name}")

config['assemblies'] = unique_assemblies

# Remove duplicate tracks
seen_tracks = set()
unique_tracks = []
for track in config['tracks']:
    track_id = track.get('trackId')
    if track_id not in seen_tracks:
        seen_tracks.add(track_id)
        unique_tracks.append(track)
    else:
        print(f"Removing duplicate track: {track_id}")

config['tracks'] = unique_tracks

with open(config_file, 'w') as f:
    json.dump(config, f, indent=2)

print(f"Fixed config - {len(unique_assemblies)} assemblies, {len(unique_tracks)} tracks")
