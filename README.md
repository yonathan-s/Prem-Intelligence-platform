# Premier League Intelligence Platform

A data engineering project on **Microsoft Fabric**. It scores every Premier League club-season since 1992/93 for how dominant it was, taking into account how strong the competition was, and publishes the result as a Power BI report.

It answers questions such as:

- Was Arsenal's unbeaten 2003/04 season more dominant than Manchester City's 100-point 2017/18 season?
- Does winning the Premier League mean more when other English clubs are performing strongly in Europe?
- Which Premier League team achieved the greatest dominance relative to its competition?

> **Status:** the medallion pipeline (Bronze, Silver, Gold) runs in PySpark on a Fabric Lakehouse, and a Power BI semantic model and report sit on top of it. Orchestration, Git integration and CI, and SCD Type 2 are not built yet. See the [roadmap](#roadmap).

---

## Historical Dominance Index (HDI)

The HDI combines three independent measures, each normalised to 0-100:

| Component | Measures |
|---|---|
| **D**: domestic dominance | Points per game, win rate, goal difference per game, title margin |
| **E**: European strength | How well *other* English clubs performed in Europe. The club being scored is excluded so it isn't rewarded twice for its own success. |
| **C**: domestic parity | How closely matched the rest of the league was |

$$
L = 0.75E + 0.25C
$$

$$
HDI = D \times \left(0.80 + 0.20 \cdot \frac{L}{100}\right)
$$

The formula and weights are provisional. Every weight lives in [fabric/config/reference_data.json](fabric/config/reference_data.json), and each score row records the model version that produced it. The highest score is Manchester City 2017/18 at 89.68.

### How each part is calculated

- **D (domestic dominance):** a weighted average of points per game (30%), win rate (20%), goal difference per game (25%) and title margin (25%). The title margin is points per game ahead of the strongest rival: the gap to the runner-up for the champion, and the (negative) gap to the champion for everyone else. Each measure is scaled from 0 to 100 across all 686 club-seasons first, and everything is per game, so the 42-game seasons (1992/93 to 1994/95) are comparable with the 38-game ones.
- **E (European strength):** for each club-season, how far the other Premier League clubs got in Europe. Each competition round has a rank (0 for qualifying up to 10 for winning the competition) multiplied by a strength factor for the competition (Champions League 1.0, Europa League and UEFA Cup 0.6, Cup Winners' Cup 0.5, Conference League 0.4). Each club is judged on the competition where it got closest to the top. E is the other clubs' points as a percentage of the most they could have earned, so a season with more European places is not scored higher just for having more clubs, and a Champions League club counts for more than a Europa League one.
- **C (domestic parity):** the spread (standard deviation) of the other clubs' points per game that season, scaled so the most evenly matched league scores 100.

Known limits: points deductions are not applied, so positions in the four seasons that had them (Middlesbrough 1996/97, Portsmouth 2009/10, Everton 2023/24, Nottingham Forest 2023/24) follow raw points; the 1992/93 Champions League pre-group rounds are scored approximately.

