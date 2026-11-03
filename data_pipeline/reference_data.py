"""Hand-written reference data: seasons, competitions, and the fixes the two CSVs need."""

# One row per Premier League season. There were 22 clubs until 1994/95, then 20.
SEASONS = [
    {
        "start_year": year,
        "label": f"{year}/{(year + 1) % 100:02d}",
        "teams_in_league": 22 if year <= 1994 else 20,
        "games_per_team": 42 if year <= 1994 else 38,
    }
    for year in range(1992, 2026)
]

# The five European competitions that exist from 1992/93. strength_factor stays empty until
# the scoring weights are decided.
COMPETITIONS = [
    {"code": "CL", "name": "Champions League", "strength_factor": None},
    {"code": "EL", "name": "Europa League", "strength_factor": None},
    {"code": "UC", "name": "UEFA Cup", "strength_factor": None},
    {"code": "CWC", "name": "Cup Winners' Cup", "strength_factor": None},
    {"code": "ECL", "name": "Conference League", "strength_factor": None},
]

# The UEFA file's spelling -> the team file's spelling, only where they differ.
UEFA_NAME_TO_CLUB = {
    "Tottenham Hotspur": "Tottenham",
    "Newcastle United": "Newcastle",
    "Leeds United": "Leeds",
    "West Ham United": "West Ham",
    "Leicester City": "Leicester",
    "Blackburn Rovers": "Blackburn",
    "Bolton Wanderers": "Bolton",
    "Wolverhampton Wanderers": "Wolves",
    "Ipswich Town": "Ipswich",
    "Stoke City": "Stoke",
    "Swansea City": "Swansea",
    "Birmingham City": "Birmingham",
    "Brighton & Hove Albion": "Brighton",
    "Norwich City": "Norwich",
    "Wigan Athletic": "Wigan",
    "Cardiff City": "Cardiff",
}

# English clubs in Europe that never played in the Premier League, so they are not in the team file.
ENGLISH_CLUBS_NOT_IN_PREMIER_LEAGUE = {"Millwall", "Wrexham"}

# The same round written two ways in the UEFA file.
ROUND_SPELLING_FIXES = {"1/2 final": "1/2 Final", "Play-Offs": "Play-offs"}

# The UEFA file has no penalties column. These are the English finals that finished level,
# keyed by (start year, competition), with the team that won the shootout.
SHOOTOUT_WINNERS = {
    (1999, "UC"): "Galatasaray",
    (2004, "CL"): "Liverpool",
    (2007, "CL"): "Manchester United",
    (2011, "CL"): "Chelsea",
    (2020, "EL"): "Villarreal",
    (2025, "CL"): "Paris Saint-Germain",
}
