
# Create the 'odis' table with the specified columns
# similarly create t20s and test tables (i considered t20 and ipl as same t20 format)
CREATE TABLE t20s (
    city VARCHAR(50),
    date DATE,
    event_name VARCHAR(100),
    match_number VARCHAR(50),
    match_type VARCHAR(50),
    venue VARCHAR(100),
    winner VARCHAR(100),
    win_by_runs INT,
    overs INT,
    player_of_the_match VARCHAR(100),
    season VARCHAR(100),
    teams VARCHAR(100)
);
