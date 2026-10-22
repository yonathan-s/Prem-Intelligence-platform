from ingestion import sources
from ingestion.run import validate_football_data


def test_season_label_and_length():
    assert sources.season_label(1999) == "1999/00"
    assert sources.matches_per_team(1994) == 42
    assert sources.matches_per_team(1995) == 38


def test_parse_football_data_csv_drops_blank_rows_and_columns():
    text = "Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,,\nE0,14/08/93,Arsenal,Coventry,0,3,A,,\n,,,,,,,,\n"
    rows = sources.parse_football_data_csv(text, 1993)
    assert rows == [
        {
            "Div": "E0", "Date": "14/08/93", "HomeTeam": "Arsenal", "AwayTeam": "Coventry",
            "FTHG": "0", "FTAG": "3", "FTR": "A", "season": "1993/94", "date_iso": "1993-08-14",
        }
    ]


def test_date_parsing_handles_two_and_four_digit_years():
    assert sources.parse_football_data_date("19/08/00") == "2000-08-19"
    assert sources.parse_football_data_date("16/08/2003") == "2003-08-16"
    assert sources.parse_football_data_date("garbage") is None


def test_engsoccerdata_filters_by_season_and_maps_na():
    text = 'Season,tier,pens\n1991,1,NA\n1992,1,NA\n'
    rows = sources.parse_engsoccerdata_csv(text)
    assert rows == [{"Season": "1992", "tier": "1", "pens": None}]


def test_validation_flags_wrong_match_count():
    match = {"HomeTeam": "A", "AwayTeam": "B", "date_iso": "2000-01-01"}
    seasons = {"1999/00": [match] * 10, "2000/01": [match] * 380}
    problems = validate_football_data(seasons)
    assert any("1999/00" in p for p in problems)
