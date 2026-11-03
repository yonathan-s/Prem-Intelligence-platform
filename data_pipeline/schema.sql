-- Tables from the approved ERD. round_rank and season_index come later with the scoring.

CREATE TABLE IF NOT EXISTS season (
    start_year      integer PRIMARY KEY,
    label           text    NOT NULL,
    teams_in_league integer NOT NULL,
    games_per_team  integer NOT NULL
);

CREATE TABLE IF NOT EXISTS club (
    club_id  integer PRIMARY KEY,
    name     text NOT NULL UNIQUE,
    alt_name text
);

CREATE TABLE IF NOT EXISTS competition (
    code            text PRIMARY KEY,
    name            text NOT NULL,
    strength_factor numeric(4, 2)
);

CREATE TABLE IF NOT EXISTS team_season (
    season_start    integer NOT NULL REFERENCES season (start_year),
    club_id         integer NOT NULL REFERENCES club (club_id),
    position        integer NOT NULL,
    played          integer NOT NULL,
    won             integer NOT NULL,
    drawn           integer NOT NULL,
    lost            integer NOT NULL,
    goals_for       integer NOT NULL,
    goals_against   integer NOT NULL,
    goal_difference integer NOT NULL,
    points          integer NOT NULL,
    is_champion     boolean NOT NULL,
    is_relegated    boolean NOT NULL,
    PRIMARY KEY (season_start, club_id)
);

CREATE TABLE IF NOT EXISTS uefa_match (
    match_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    match_date      date    NOT NULL,
    season_start    integer NOT NULL REFERENCES season (start_year),
    competition     text    NOT NULL REFERENCES competition (code),
    round           text    NOT NULL,
    home_name       text    NOT NULL,
    away_name       text    NOT NULL,
    home_club_id    integer REFERENCES club (club_id),
    away_club_id    integer REFERENCES club (club_id),
    home_goals      integer NOT NULL,
    away_goals      integer NOT NULL,
    home_country    text    NOT NULL,
    away_country    text    NOT NULL,
    home_goals_ht   integer,
    away_goals_ht   integer,
    is_walkover     boolean NOT NULL,
    shootout_winner text
);

-- Supabase exposes every table in the public schema through its API keys. With row level
-- security on and no policies, those keys can't read or write these tables. This pipeline
-- connects as the database owner, which is not affected.
ALTER TABLE season      ENABLE ROW LEVEL SECURITY;
ALTER TABLE club        ENABLE ROW LEVEL SECURITY;
ALTER TABLE competition ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_season ENABLE ROW LEVEL SECURITY;
ALTER TABLE uefa_match  ENABLE ROW LEVEL SECURITY;
