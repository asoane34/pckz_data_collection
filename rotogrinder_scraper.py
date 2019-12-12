import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import sys

BASE_WEATHER = 'https://rotogrinders.com/weather/nfl'

class RotoCollector():
    def __init__(self, base_weather = BASE_WEATHER):
        self.base_weather = base_weather
        self.weather_storage = []
    
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


