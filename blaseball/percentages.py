from os import environ
from sqlalchemy import create_engine
import pandas as pd

# PSQL_CONNECTION_STRING isn't formatted how we want it, so parse the params in dict format
# and make a brand-new shiny connection string
connection_list = environ.get('PSQL_CONNECTION_STRING').split(";")
connection_dict = {
    k: v
    for (k, v) in [tuple(map(str, x.split('='))) for x in connection_list]
}
connection_string = "postgres+psycopg2://{d[username]}:{d[password]}@{d[Host]}/{d[database]}".format(
    d=connection_dict)
sqlengine = create_engine(connection_string)

# Get everything we're interested in — all current regular season games (not postseason)
# We can do the rest of the processing locally. remember seasons are 0-indexed, so 6 is really 7
games_df = pd.read_sql(
    "SELECT * FROM data.games WHERE season=6 AND is_postseason = FALSE", con=sqlengine, index_col="game_id")

# We also need a list of teams and team IDs to iterate through
teams_df = pd.read_sql(
    "SELECT * FROM data.teams_current", con=sqlengine, index_col="team_id")

# A team's "disappointment percentage" is numGamesFavoredLost / numGamesFavored.
# a team is favored if [home, away]_odds > 0.5


def team_disappointment_num_games(team_id):
    # Get all games where team_id is favored, and then make another DF with only the losing games
    # returns disappointments and favored wins
    favored_df = games_df.query(
        "(home_team == @team_id and home_odds > 0.5) or (away_team == @team_id and away_odds > 0.5)")
    lost_favored_df = favored_df.query(
        "(home_team == @team_id and home_score < away_score) or (away_team == @team_id and away_score < home_score)")
    return (len(lost_favored_df), len(favored_df)-len(lost_favored_df))


def team_disappointment_percentage(team_id):
    x, y = team_disappointment_num_games(team_id)
    return x/(x+y)


def team_surprise_num_games(team_id):
    # this time, it's all the games where the team was the underdog and won anyway
    # returns surprises and underdog losses
    underdog_df = games_df.query(
        "(home_team == @team_id and home_odds < 0.5) or (away_team == @team_id and away_odds < 0.5)")
    won_underdog_df = underdog_df.query(
        "(home_team == @team_id and home_score > away_score) or (away_team == @team_id and away_score > home_score)")
    return (len(won_underdog_df), len(underdog_df)-len(won_underdog_df))


def team_surprise_percentage(team_id):
    x, y = team_surprise_num_games(team_id)
    return x/(x+y)


# target_team_id = "8d87c468-699a-47a8-b40d-cfb73a5660ad"

# print("surprise percentage: {}".format(team_surprise_percentage(target_team_id)))


def all_teams_percentages():
    disappointment_dict = {}
    surprise_dict = {}
    for team_id in teams_df.index:
        disappointment_dict[team_id] = team_disappointment_num_games(
            team_id) + (team_disappointment_percentage(team_id),)
        surprise_dict[team_id] = team_surprise_num_games(
            team_id) + (team_surprise_percentage(team_id),)
    disappointment_df = pd.DataFrame.from_dict(disappointment_dict, orient='index', columns=[
        'disappointments', 'favored_wins', 'disappointment_percentage'])
    surprise_df = pd.DataFrame.from_dict(surprise_dict, orient='index', columns=[
        'surprises', 'underdog_losses', 'surprise_percentage'])
    results_df = disappointment_df.join(surprise_df)
    return results_df


output_df = teams_df[['nickname']].join(all_teams_percentages())
output_df.sort_values(by='disappointment_percentage').to_csv("percentages.csv")