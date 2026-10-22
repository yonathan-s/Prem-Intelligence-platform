from ingestion import wikipedia as w

MAIN_PAGE = """\
| champion_other = {{fbaicon|ESP}} [[Valencia CF|Valencia]]
| second_other   = {{fbaicon|FRA}} [[Olympique de Marseille|Marseille]]
==Teams==
|{{fbaicon|ENG}} [[Liverpool F.C.|Liverpool]]
==First round==
{{main|2003–04 UEFA Cup first round}}
{{#lst:2003–04 UEFA Cup first round|R1}}
==Final phase==
===Semi-finals===
{{#lst:2003–04 UEFA Cup final phase|SF}}
==Top goalscorers==
{{fbaicon|ENG}} [[Newcastle United F.C.|Newcastle United]]
"""

SUBPAGES = {
    "2003–04 UEFA Cup first round": "<section begin=R1 />\n|[[Newcastle United F.C.|Newcastle United]]|ENG|6–0|[[NAC Breda]]|NED|5–0|1–0\n<section end=R1 />",
    "2003–04 UEFA Cup final phase": "<section begin=SF />\n|{{fbaicon|ENG}} [[Newcastle United F.C.|Newcastle United]]|1|0\n<section end=SF />",
}


def test_english_clubs_handles_both_markups():
    assert w.english_clubs("{{fbaicon|ENG}} [[Arsenal F.C.|Arsenal]]") == {"Arsenal"}
    assert w.english_clubs("'''{{fbaicon|ENG}} [[Leeds United A.F.C.|Leeds]]'''") == {"Leeds United"}
    assert w.english_clubs("|[[Chelsea F.C.|Chelsea]]|ENG|2–0|[[Celtic F.C.|Celtic]]|SCO") == {"Chelsea"}
    assert w.english_clubs("|[[Celtic F.C.|Celtic]]|SCO|1–3|[[Villarreal CF|Villarreal]]|ESP") == set()


def test_stage_for_maps_headings_in_tournament_terms():
    assert w.stage_for("Group A") == "Group stage"
    assert w.stage_for("Second group stage") == "Second group stage"
    assert w.stage_for("Third qualifying round") == "Qualifying"
    assert w.stage_for("Knockout phase play-offs") == "KO play-off"
    assert w.stage_for("Quarter-finals") == "QF"
    assert w.stage_for("Final") == "F"
    assert w.stage_for("Final phase") is None
    assert w.stage_for("Top goalscorers") is None


def test_parse_season_page_takes_furthest_stage_and_ignores_non_stage_sections():
    problems: list[str] = []
    result = w.parse_season_page(MAIN_PAGE, SUBPAGES.get, problems)
    assert result["Newcastle United"]["round_reached"] == "SF"
    assert result["Liverpool"]["round_reached"] == "Entered"
    assert problems == []


def test_infobox_overrides_stage_for_finalists():
    page = "| champion = {{fbaicon|ENG}} [[Chelsea F.C.|Chelsea]]\n| second_other = {{fbaicon|ENG}} [[Arsenal F.C.|Arsenal]]\n==Final==\n"
    result = w.parse_season_page(page, lambda t: None, [])
    assert result["Chelsea"]["round_reached"] == "Winners"
    assert result["Arsenal"]["round_reached"] == "Runners-up"


def test_missing_label_falls_back_to_whole_page_and_reports():
    problems: list[str] = []
    page = "==Group stage==\n{{#lst:Sub|missing}}\n"
    result = w.parse_season_page(page, lambda t: "{{fbaicon|ENG}} [[Arsenal F.C.|Arsenal]]", problems)
    assert result["Arsenal"]["round_reached"] == "Group stage"
    assert any("no section 'missing'" in p for p in problems)


def test_season_title():
    assert w.season_title(1999) == "1999–2000"
    assert w.season_title(2003) == "2003–04"
