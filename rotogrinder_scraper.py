import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import sys
import re
import os

BASE_WEATHER = 'https://rotogrinders.com/weather/nfl'
BASE_BETTING = 'https://rotogrinders.com/nfl/odds'
OUTPUT_DIR = 'NFL_data'


class RotoCollector():
    def __init__(self, base_weather = BASE_WEATHER, base_betting = BASE_BETTING, output_dir = OUTPUT_DIR):
        self.base_weather = base_weather
        self.base_betting = base_betting
        self.output_dir = output_dir
        self.weather_storage = []
        self.betting_storage = []
    
    
    def roto_nfl_scrape(self):
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)
            self.collect_weather()
            self.collect_betting_lines()
            self.write_to_file()
            print('NFL data sucessfully scraped.')

    def write_to_file(self):
        weather_frame = pd.DataFrame(self.weather_storage)
        betting_frame = pd.DataFrame(self.betting_storage)
        final_frame = weather_frame.merge(betting_frame, how = 'left', left_on = ['road_team', 'home_team'],
                                                                        right_on = ['road_team', 'home_team'])
        final_frame.to_csv(self.output_dir + '/12-12-19.csv', index = False)
        
    def collect_weather(self):
        try:
            weather_page = requests.get(self.base_weather)
            weather_soup = BeautifulSoup(weather_page.content, 'html.parser')
        except:
            print('{} error has occurred'.format(sys.exc_info()[0]))
            raise
        game_data = weather_soup.findAll('div', {'class' : "blk crd"})
        for game in game_data:
            weather_dict = self.initialize_weather()
            meta_data = game.find('header', {'class' : "hdr"})
            teams = [team.get_text() for team in meta_data.findAll('span', {'class' : "shrt"})]
            weather_dict['road_team'] = teams[0]
            weather_dict['home_team'] = teams[1]
            time_place = meta_data.find('span', {'class' : "time"}).get_text().strip().split('\n')
            weather_dict['game_time'] = time_place[0]
            weather_dict['game_location'] = time_place[1].lstrip()
            point_total = meta_data.find('span', {'class' : "overunder"}).get_text().strip().split(' ')[0]
            weather_dict['point_total'] = point_total
            if not game.find('div', {'class' : "blk current-forecast"}):
                weather_dict['temperature (°F)'] = 'DOME'
                weather_dict['precipitation_chance'] = 'DOME'
                weather_dict['wind_direction'] = 'DOME'
                weather_dict['wind_speed'] = 'DOME'
            else:
                weather_values = [value.get_text() for value in game.find('div', {'class' : "blk current-forecast"}).findAll('span', {'class' : "value"})]
                weather_dict['temperature (°F)'] = weather_values[0]
                weather_dict['precipitation_chance'] = weather_values[1]
                weather_dict['wind_direction'] = weather_values[2]
                weather_dict['wind_speed'] = weather_values[3]
            self.weather_storage.append(weather_dict)
    
    @staticmethod
    def initialize_weather():
        weather_dict = {'road_team' : None,
                        'home_team' : None,
                        'game_time' : None,
                        'game_location' : None,
                        'point_total' : None,
                        'temperature (°F)' : None,
                        'precipitation_chance' : None,
                        'wind_direction' : None,
                        'wind_speed' : None}
        return(weather_dict)

    def collect_betting_lines(self):
        try:
            odds_page = requests.get(self.base_betting)
            odds_soup = BeautifulSoup(odds_page.content, 'html.parser')
        except:
            print('{} error has occured'.format(sys.exc_info()[0]))
            raise
        data_tables = odds_soup.findAll('div', {'class' : "tbl-body"})
        team_names = data_tables[0].findAll('div', {'class' : "row"})
        dk_lines = data_tables[1].findAll('div', {'class' : "sb data card-data"})
        fd_lines = data_tables[2].findAll('div', {'class' : "sb data card-data"})
        pb_lines = data_tables[3].findAll('div', {'class' : "sb data card-data"})
        
        all_lines = [dk_lines, fd_lines, pb_lines]
        all_books = ['DK', 'FD', 'PB']
        for n in range(len(team_names)):
            lines_dict = {}
            lines_dict['road_team'] = team_names[n].findAll('strong')[1].get_text()
            lines_dict['home_team'] = team_names[n].findAll('strong')[2].get_text()
            
            for i in range(3):
                game_total = [total.get_text().strip().lstrip() for total in all_lines[i][3 * n].findAll('span')][0]
                game_spreads = [spread.get_text().strip().lstrip() for spread in all_lines[i][3 * n + 2].findAll('span')]
                road_total, home_total = self.derive_team_totals(game_total, game_spreads[0])
                game_moneylines = [moneyline.get_text().strip().lstrip() for moneyline in all_lines[i][3 * n + 1].findAll('span')]
                lines_dict[all_books[i] + '_total'] = game_total
                lines_dict[all_books[i] + '_team_total_rd'] = road_total
                lines_dict[all_books[i] + '_team_total_hm'] = home_total
                lines_dict[all_books[i] + '_moneyline_rd'] = game_moneylines[0]
                lines_dict[all_books[i] + '_moneyline_hm'] = game_moneylines[1]
                lines_dict[all_books[i] + '_spread_rd'] = game_spreads[0]
                lines_dict[all_books[i] + '_spread_hm'] = game_spreads[1]
            self.betting_storage.append(lines_dict)
    
    @staticmethod
    def derive_team_totals(game_total, road_spread):
        game_total = game_total.split(' ')[0]
        road_spread = road_spread.split(' ')[0]
        line_format_1 = '[\d]+\.\d'
        line_format_2 = '[\d]+'
        try:
            game_total = float(re.findall(line_format_1, game_total)[0])
        except Exception:
            game_total = float(re.findall(line_format_2, game_total)[0])
        if road_spread[0] == '+':
            road_spread = float(road_spread.replace('+', ''))
            road_total = (game_total / 2) - (road_spread / 2)
            home_total = game_total - road_total
        elif road_spread[0] == '-':
            road_spread = float(road_spread)
            road_total = (game_total / 2) - (road_spread / 2)
            home_total = game_total - road_total
        else:
            road_total = game_total / 2
            home_total = game_total / 2
        return(road_total, home_total)



