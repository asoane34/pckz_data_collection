import requests 
from bs4 import BeautifulSoup, Comment
import os
import datetime
import sys
import sqlite3 as sql
import pandas as pd 

class BoxScoreCreator():
    def __init__(self, db, date, team1, team2, starter1, starter2):
        self.db = db
        self.date = date
        self.team1 = team1
        self.team2 = team2
        self.starter1 = starter1
        self.starter2 = starter2
        self.projected = []
    
    @staticmethod
    def get_batting(team, connection, cursor):
        team_dict = {}
        team_stats = '''
        SELECT AVG(H), AVG(R), (SUM(baserunners) / SUM(PA)) + (SUM(total_bases) / SUM(AB)), (SUM(H_A) + SUM(BB)) / SUM(IP),
        SUM(ER) / SUM(IP)
        FROM Team_Statistics
        WHERE team_name = ?
        '''
        cursor.execute(team_stats, (team,))
        for tup in cursor:
            values = tup
        team_dict['avg_hits'] = values[0]
        team_dict['avg_runs'] = values[1]
        team_dict['OBPS'] = values[2]
        team_dict['relief_WHIP'] = values[3]
        team_dict['relief_RPI'] = values[4]
        return(team_dict)

    @staticmethod
    def get_starting_pitching(starter, connection, cursor):
        starting_pitcher = {}
        starting_pitching_query = '''
        SELECT AVG(IP), SUM(H_A) / SUM(IP), SUM(ER) / SUM(IP), (SUM(H_A) + SUM(BB)) / SUM(IP)
        FROM Starting_Pitching
        WHERE name = ? 
        '''
        cursor.execute(starting_pitching_query, (starter,))
        for tup in cursor:
            values = tup
        starting_pitcher['avg_IP'] = values[0]
        starting_pitcher['avg_RPI'] = values[1]
        starting_pitcher['WHIP'] = values[2]
        return(starting_pitcher)


class BoxScoreCollector():
    def __init__(self, date):
        self.date = date
        self.box_base = 'https://www.baseball-reference.com/'
        self.daily_page_base = 'https://www.baseball-reference.com/boxes/?date={}-{}-{}'
        self.daily_links = []
        self.daily_starting_pitching = []
        self.daily_team_stats = []
    
    def update_records(self):
        team_df = pd.DataFrame(self.daily_team_stats)
        starters_df = pd.DataFrame(self.daily_starting_pitching)
        try:
            conn = sql.connect('baseball_ref.db')
            c = conn.cursor()
        except:
            print('COULD NOT ACCESS DATABASE: {}'.format(sys.exc_info()[0]))
            raise
        c.execute('''
                CREATE TABLE IF NOT EXISTS
                Team_Statistics(team_name VARCHAR (25), AB INTEGER, R INTEGER, H DOUBLE, PA INTEGER, onbase_perc FLOAT, 
                slugging_perc FLOAT, total_bases FLOAT, baserunners FLOAT, date VARCHAR (10),IP DOUBLE, 
                H_A DOUBLE, ER DOUBLE, BB DOUBLE, SO DOUBLE)
                ''')
        c.execute('''
                CREATE TABLE IF NOT EXISTS
                Starting_Pitching(name VARCHAR (30), retrosheet_id VARCHAR (10), IP DOUBLE, H_A DOUBLE, ER DOUBLE, BB DOUBLE,
                SO DOUBLE, date VARCHAR (10))
                ''')
        team_df.to_sql('Team_Statistics', conn, if_exists = 'append', index = False)
        starters_df.to_sql('Starting_Pitching', conn, if_exists = 'append', index = False)
        conn.commit()
        conn.close()

    def daily_scrape(self):
        home_url = self.build_query(self.daily_page_base, self.date)
        try:
            home_page = requests.get(home_url).content
            home_soup = BeautifulSoup(home_page, 'html.parser')
        except:
            print('There was a problem retrieving data for {}'.format(self.date))
            raise
        self.get_links(home_soup)
        for link in self.daily_links:
            box_url = self.box_base + link
            game = Game(box_url, self.date)
            try:
                game.scrape_box()
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print('There was a problem retrieving data for {}, error: {}'.format(box_url, sys.exc_info()[0]))
                continue
            self.daily_starting_pitching.append(game.away_starter_stats)
            self.daily_starting_pitching.append(game.home_starter_stats)
            self.daily_team_stats.append(game.away_team_stats)
            self.daily_team_stats.append(game.home_team_stats)
        
    def get_links(self, box_soup):
        box_scores = box_soup.find('div', {'class' : 'game_summaries'}).findAll('table', {'class' : 'teams'})
        for box in box_scores:
            a_tags = box.findAll('a')
            for a in a_tags:
                if '/boxes/' in a['href']:
                    self.daily_links.append(a['href'])
                    break

    @staticmethod
    def build_query(base, date):
        return(base.format(date.strftime('%Y'), date.strftime('%m'), date.strftime('%d')))
    

class Game():
    def __init__(self, url, date):
        self.url = url
        self.date = date
        self.away_team = None
        self.home_team = None
        self.away_team_stats = None
        self.home_team_stats = None
        self.away_starter_stats = None
        self.home_starter_stats = None
    
    def scrape_box(self):
        game_page = requests.get(self.url).content
        game_soup = BeautifulSoup(game_page, 'html.parser')
        teams = [i.get_text() for i in game_soup.find('div', {'class' : 'scorebox'}).findAll('a', {'itemprop' : 'name'})]
        self.away_team, self.home_team = teams[0], teams[1]
        self.away_team_stats = self.collect_batting(self.away_team, game_soup)
        self.home_team_stats = self.collect_batting(self.home_team, game_soup)
        self.away_team_stats.update({'date' : self.date})
        self.home_team_stats.update({'date' : self.date})
        comment_wrappers = game_soup.findAll('div', {'class' : 'section_wrapper setup_commented commented'})
        for wrapper in comment_wrappers:
            if wrapper.find('span', {'data-label' : 'Pitching Lines and Info'}):
                all_pitching = BeautifulSoup(wrapper.find(text = lambda text: isinstance(text, Comment)), 'html.parser')
        self.away_starter_stats, away_relief_pitching = self.collect_pitching(self.away_team, all_pitching)
        self.home_starter_stats, home_relief_pitching = self.collect_pitching(self.home_team, all_pitching)
        self.away_team_stats.update(away_relief_pitching)
        self.home_team_stats.update(home_relief_pitching)
        self.away_starter_stats.update({'date' : self.date})
        self.home_starter_stats.update({'date' : self.date})
    
    @staticmethod
    def collect_batting(team, soup):
        team = team.replace(' ', '').replace('.', '')
        to_collect = ['AB', 'R', 'H', 'PA', 'onbase_perc', 'slugging_perc']
        batting_dict = {'team_name' : team}
        tag_ID = 'all_' + team + 'batting'
        team_batting = BeautifulSoup(soup.find('div', {'id' : tag_ID}).find(text = lambda text: isinstance(text, Comment)), 'html.parser').find('tfoot')
        for stat in to_collect:
            batting_dict[stat] = team_batting.find('td', {'data-stat' : stat}).get_text().strip()
        batting_dict['total_bases'] = int(batting_dict['AB']) * float(batting_dict['slugging_perc'])
        batting_dict['baserunners'] = int(batting_dict['PA']) * float(batting_dict['onbase_perc'])
        return(batting_dict)

    @staticmethod
    def collect_pitching(team, pitching_soup):
        team = team.replace(' ', '').replace('.', '')
        to_collect = ['IP', 'H', 'ER', 'BB', 'SO']
        starting_pitcher_dict = {}
        relief_pitching_dict = {}
        tag_ID = 'all_' + team + 'pitching'
        starting_pitching = pitching_soup.find('div', {'id' : tag_ID}).find('tbody').findAll('tr')[0]
        team_totals = pitching_soup.find('div', {'id' : tag_ID}).find('tfoot')
        starting_pitcher_retro_id = starting_pitching.find('th')['data-append-csv']
        starting_pitcher_name = starting_pitching.find('th', {'data-stat' : 'player'}).get_text().split(', ')[0]
        starting_pitcher_dict['name'] = starting_pitcher_name
        starting_pitcher_dict['retrosheet_id'] = starting_pitcher_retro_id
        for stat in to_collect:
            if stat == 'H':
                hits_surrendered = float(starting_pitching.find('td', {'data-stat' : stat}).get_text().strip())
                starting_pitcher_dict['H_A'] = hits_surrendered
                relief_pitching_dict['H_A'] = float(team_totals.find('td', {'data-stat' : stat}).get_text().strip()) - \
                    hits_surrendered
            else:
                starting_pitcher_dict[stat] = float(starting_pitching.find('td', {'data-stat' : stat}).get_text().strip())
                relief_pitching_dict[stat] = float(team_totals.find('td', {'data-stat' : stat}).get_text().strip()) - \
                starting_pitcher_dict[stat]
        return(starting_pitcher_dict, relief_pitching_dict)




    