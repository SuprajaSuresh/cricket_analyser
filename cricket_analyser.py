import pandas as pd
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

# Set up the database connection
DB_CONFIG = {
    'user': 'root',
    'password': 'root123$',
    'host': 'localhost',
    'port': 3306,
    'database': 'cricket_analyser'
}
ENGINE = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

DATA_FOLDER = 'data'
TABLE_FOLDER_MAP = {
    'ipl_json': 't20s',
    't20_json': 't20s',
    'odis_json': 'odis',
    'tests_json': 'tests'
}

def extract_match_info(data, filename):
    info = data.get('info', {})
    event = data.get('event', {})
    outcome = info.get('outcome', {})
    outcome_by = outcome.get('by', {})
    teams = info.get('teams', [])
    players = info.get('players', {})
    match_id = os.path.splitext(filename)[0]
    return {
        'match_id': match_id,
        'city': info.get('city', 'Unknown City'),
        'date': info.get('dates', ['Unknown Date'])[0],
        'venue': info.get('venue', 'Unknown Venue'),
        'event_name': event.get('name', 'Unknown Event'),
        'match_number': event.get('match_id', 'Unknown Match ID'),
        'match_type': info.get('match_type', 'Unknown Match Type'),
        'winner': outcome.get('winner', 'Unknown Winner'),
        'win_by_runs': outcome_by.get('runs', 0),
        'overs': info.get('overs', 0),
        'player_of_the_match': ', '.join(outcome.get('player_of_the_match', [])) if isinstance(outcome.get('player_of_the_match', []), list) else outcome.get('player_of_the_match', 'Unknown Player'),
        'season': info.get('season', 'Unknown Season'),
        'teams': ', '.join(teams) if isinstance(teams, list) else str(teams),
        'team_1_players': ', '.join(players.get(teams[0], [])) if isinstance(teams, list) and len(teams) > 0 else 'Unknown Team 1',
        'team_2_players': ', '.join(players.get(teams[1], [])) if isinstance(teams, list) and len(teams) > 1 else 'Unknown Team 2'
    }

def extract_deliveries(data, match_id):
    deliveries_list = []
    innings = data.get('innings', [])
    for inning_number, inning in enumerate(innings, 1):
        team = inning.get('team', 'Unknown Team')
        for over in inning.get('overs', []):
            over_number = over.get('over')
            for delivery in over.get('deliveries', []):
                extras = delivery.get('extras', {})
                wickets = delivery.get('wickets', [])
                deliveries_list.append({
                    'match_id': match_id,
                    'inning': inning_number,
                    'batting_team': team,
                    'over': over_number,
                    'batter': delivery.get('batter'),
                    'bowler': delivery.get('bowler'),
                    'non_striker': delivery.get('non_striker'),
                    'runs_batter': delivery.get('runs', {}).get('batter'),
                    'runs_extras': delivery.get('runs', {}).get('extras'),
                    'runs_total': delivery.get('runs', {}).get('total'),
                    'extras_type': ', '.join(extras.keys()) if extras else None,
                    'extras_detail': json.dumps(extras) if extras else None,
                    'wicket_kind': wickets[0]['kind'] if wickets else None,
                    'player_out': wickets[0]['player_out'] if wickets else None,
                    'fielder': (
                        wickets[0]['fielders'][0]['name']
                        if wickets and 'fielders' in wickets[0] and wickets[0]['fielders']
                        and isinstance(wickets[0]['fielders'][0], dict) and 'name' in wickets[0]['fielders'][0]
                        else (
                            wickets[0]['fielders'][0]
                            if wickets and 'fielders' in wickets[0] and wickets[0]['fielders']
                            and isinstance(wickets[0]['fielders'][0], str)
                            else None
                        )
                    )
                })
    return deliveries_list

def process_json_file(json_path, filename, table_name):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    match_info = extract_match_info(data, filename)
    df_match = pd.json_normalize([match_info])
    # Save match info
    try:
        df_match.to_sql(table_name, con=ENGINE, if_exists='append', index=False)
    except IntegrityError:
        print(f"Duplicate entry for match_id {match_info['match_id']}, skipping.")
    # Save deliveries
    deliveries = extract_deliveries(data, match_info['match_id'])
    if deliveries:
        df_deliveries = pd.DataFrame(deliveries)
        df_deliveries.to_sql('deliveries', con=ENGINE, if_exists='append', index=False)

def main():
    for subdir, dirs, files in os.walk(DATA_FOLDER):
        folder_name = os.path.basename(subdir)
        table_name = TABLE_FOLDER_MAP.get(folder_name)
        if not table_name:
            continue
        for filename in files:
            if filename.endswith('.json'):
                print(f"Processing file: {filename} in folder: {folder_name}")
                json_path = os.path.join(subdir, filename)
                process_json_file(json_path, filename, table_name)

if __name__ == "__main__":
    main()