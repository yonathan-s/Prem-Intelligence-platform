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

# The five European competitions that exist from 1992/93. strength_factor is how much a
# competition counts compared with the Champions League (1.0): a round in a competition is
# worth its rank multiplied by this number.
COMPETITIONS = [
    {"code": "CL", "name": "Champions League", "strength_factor": 1.0},
    {"code": "EL", "name": "Europa League", "strength_factor": 0.6},
    {"code": "UC", "name": "UEFA Cup", "strength_factor": 0.6},
    {"code": "CWC", "name": "Cup Winners' Cup", "strength_factor": 0.5},
    {"code": "ECL", "name": "Conference League", "strength_factor": 0.4},
]

# ---- How far along a competition each round is -----------------------------------------------
# Rank 0 means the club never reached the main competition; 10 means it won it. Late rounds are
# worth disproportionately more, so the gaps grow towards the top.
ROUND_RANKS = {
    # Qualifying rounds and play-offs to get into the group stage: not in the main competition yet.
    "Qualifying Round": 0,
    "First qualifying round": 0,
    "Second qualifying round": 0,
    "Third qualifying round": 0,
    "Play-offs": 0,
    # In the group stage or league phase.
    "Group stage": 1,
    "Group stage 1": 1,
    "League phase": 1,
    # Second group stage (Champions League 1999-2003), knockout play-off round, and the round of 64.
    "Group stage 2": 2,
    "Knockout round play-offs": 2,
    "1/32 Final": 2,
    # Knockout rounds, named by the fraction of the field left.
    "1/16 Final": 3,  # round of 32
    "1/8 Final": 4,   # round of 16
    "1/4 Final": 6,   # quarter-final
    "1/2 Final": 8,   # semi-final
}

# Labels that mean something different depending on the competition. These win over ROUND_RANKS.
ROUND_RANK_EXCEPTIONS = {
    ("UC", "First Round"): 1,    # the first round of the main UEFA Cup draw
    ("CL", "First Round"): 0,    # 1992/93 only: a knockout round before the group stage
    ("EL", "First Round"): 0,    # 2009/10 only: a qualifying round
    ("CL", "Second Round"): 1,   # 1992/93 only: the last knockout round before the group stage
    ("UC", "Second Round"): 2,
    ("UC", "Group stage"): 2,    # 2004-2008: the group stage came after a first round
}

# The Final becomes one of these two, depending on who won.
RUNNER_UP_RANK = 9
WINNER_RANK = 10

# ---- Historical Dominance Index -------------------------------------------------------------
# D, the domestic score, is a weighted average of these four measures. Each one is first turned
# into a 0-100 scale by comparing every club-season in the data, best = 100, worst = 0. The
# weights add up to 1.
D_WEIGHTS = {
    "points_per_game": 0.30,
    "win_rate": 0.20,
    "goal_diff_per_game": 0.25,
    "title_margin": 0.25,  # points per game ahead of the strongest rival (negative if behind)
}

# L, the league-strength score, mixes European strength (E) with domestic parity (C).
L_EUROPE_WEIGHT = 0.75
L_PARITY_WEIGHT = 0.25

# HDI = D * (HDI_BASE + HDI_LEAGUE_SHARE * L / 100). A club with the weakest possible league
# (L = 0) keeps 80% of its domestic score; the strongest possible league (L = 100) gives 100%.
HDI_BASE = 0.80
HDI_LEAGUE_SHARE = 0.20

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
