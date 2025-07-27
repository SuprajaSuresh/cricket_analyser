
# Create the 't20s' table with the specified columns
# similarly create t20s and test tables (ipl considered t20 and ipl as same t20 format)
CREATE TABLE t20s (
    match_id VARCHAR(20) PRIMARY KEY,
    city VARCHAR(50),
    date DATE,
    venue VARCHAR(100),
    event_name VARCHAR(100),
    match_number VARCHAR(20),
    match_type VARCHAR(20),
    winner VARCHAR(50),
    win_by_runs INT,
    overs INT,
    player_of_the_match VARCHAR(100),
    season VARCHAR(20),
    teams VARCHAR(100),
    team_1_players TEXT,
    team_2_players TEXT
);