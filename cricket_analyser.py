import pandas as pd
import os
import json
from sqlalchemy import create_engine

# Set up the database connection
user = 'root'
password = 'root123$'
host = 'localhost'
port = 3306
database = 'cricket_analyser'
# engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
engine = create_engine("mysql+pymysql://root:root123$@localhost:3306/cricket_analyser")


data_folder = 'data'
table_folder_dict = {
  'ipl_json': 't20s',
  't20_json': 't20s',
  'odis_json': 'odis',
  'tests_json': 'tests'
}

dataframes = {}

# Walk through each subfolder and file in the data directory
for subdir, dirs, files in os.walk(data_folder):
  folder_name = os.path.basename(subdir)
  table_name = table_folder_dict.get(folder_name)
  if not table_name:
    continue
  for filename in files:
    print(f"Processing file: {filename} in folder: {folder_name}")
    if filename.endswith('.json'):
      json_path = os.path.join(subdir, filename)
      with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        info = data.get('info', {})
        city = info.get('city', 'Unknown City')
        date = info.get('dates', ['Unknown Date'])[0]
        venue = info.get('venue', 'Unknown Venue')
        innings = info.get('innings', [])
        officials = info.get('officials', 'Unknown Officials')
        outcome = info.get('outcome', {})
        outcome_by = outcome.get('by', {})
        win_by_runs = outcome_by.get('runs', 0)
        overs = info.get('overs', 0)
        season = info.get('season', 'Unknown Season')
        teams = info.get('teams', [])
        venue = info.get('venue', 'Unknown Venue')
        event = data.get('event', {})
        event_name = event.get('name', 'Unknown Event')
        match_number = event.get('match_id', 'Unknown Match ID')
        match_type = info.get('match_type', 'Unknown Match Type')
        selected = [
        {
          'city': city,
          'date': date,
          'venue': venue,
          'event_name': event_name,
          'match_number': match_number,
          'match_type': match_type,
          'winner': outcome.get('winner', 'Unknown Winner'),
          'win_by_runs': win_by_runs,
          'overs': overs,
          'player_of_the_match': ', '.join(outcome.get('player_of_the_match', [])) if isinstance(outcome.get('player_of_the_match', []), list) else outcome.get('player_of_the_match', 'Unknown Player'),
          'season': season,
          'teams': ', '.join(teams) if isinstance(teams, list) else str(teams)
        }
       ]
    df = pd.json_normalize(selected)
    # Save DataFrame to SQL database
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
