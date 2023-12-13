import hashlib
import requests
import datetime
import os
import glob
import pandas as pd
import time
import logging


class Marvel:

    def __init__(self, public_key, private_key):
        self.public_key = public_key
        self.private_key = private_key

    def create_directory(self):
        """ Created all needed directories
        :return: dictionary of directories
        """
        curr_path = str(os.getcwd())

        path_map = {
            'characters':
                {
                    'raw': curr_path + '/data/raw/characters/',
                    'stage': curr_path + '/data/stage/characters/'
                },
            'comics':
                {
                    'raw': curr_path + '/data/raw/comics/',
                    'stage': curr_path + '/data/stage/comics/',
                },
            'final':
                {
                    'curated': curr_path + '/data/curated/aggregations/'
                },
            'logging':
                {
                    'log': curr_path + '/data/logging/'
                }
        }

        try:
            for path, sub_path in path_map.items():
                for path_, sub_path_ in sub_path.items():
                    os.makedirs(os.path.dirname(sub_path_), exist_ok=True)
            return path_map
        except ValueError:
            logging.error(ValueError)

    def hash_params(self, timestamp):
        """ Marvel API requires server side API calls to include
        md5 hash of timestamp + public key + private key

        :return: Hashed Parameters
        """
        try:
            hash_md5 = hashlib.md5()
            hash_md5.update(f'{timestamp}{self.private_key}{self.public_key}'.encode('utf-8'))
            hashed_params = hash_md5.hexdigest()
            return hashed_params
        except ValueError:
            logging.error(ValueError)

    def get_characters(self, offset=0, limit=100):
        """ Call Characters endpoints to get characters
        :param offset:  the position in the dataset of a particular record
        :param limit: The maximum number of entries to return.
        :return: API Response
        """
        try:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')
            params = {'ts': timestamp,
                      'apikey': self.public_key,
                      'hash': self.hash_params(timestamp),
                      'limit': limit,
                      'offset': offset}
            response = requests.get('https://gateway.marvel.com/v1/public/characters',
                               params=params)

            results = response.json()
            return results
        except ValueError:
            logging.error(ValueError)

    def get_all_characters(self, out_path):
        """ Get all characters from Marvel API
        :param out_path: Location to save characters dataset
        """
        curr_offset = 0
        curr_limit = 100
        total_cnt_characters = 0

        try:
            logging.info('Pulling characters data is started..')
            print('Pulling characters data is started..')
            while curr_offset <= total_cnt_characters:
                time.sleep(10)
                if total_cnt_characters == 0:
                    raw_data = self.get_characters(curr_offset, curr_limit)
                    total_cnt_characters = raw_data['data']['total']
                    curr_offset += 100
                else:
                    raw_data = self.get_characters(curr_offset, curr_limit)
                    curr_offset += 100
                current_ts = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

                path = out_path + 'characters_' + str(curr_offset) + '_' + current_ts + '.json'
                df = pd.DataFrame(dict(raw_data)['data']['results'])
                df['current_timestamp'] = current_ts

                df.to_json(path)
                print(path, 'is created')
                logging.info(path + ' is created')

            logging.info('Pulling characters data is finished.')
            print('Pulling characters data is finished.')
        except ValueError:
            logging.error(ValueError)

    def characters_cleansing(self, in_path, out_path):
        """ Clean all raw characters data
        :param in_path: Location to read all saved characters raw data
        :param out_path: Location to save cleaned characters data
        :return: Cleaned character pandas dataframe
        """
        try:
            logging.info('Characters cleansing started..')
            print('Characters cleansing started..')
            path_to_json = in_path
            json_files = glob.glob(os.path.join(path_to_json, '*.json'))

            characters_df = pd.concat((pd.read_json(file) for file in json_files))

            characters_df['comics_count'] = characters_df['comics'].apply(lambda x: (dict(x)['available']))
            refined_characters_df = characters_df[['id', 'name', 'comics_count']].reset_index(drop=True)

            refined_characters_df.to_csv(out_path + 'cleaned_characters.csv')
            logging.info('All characters are cleaned')
            print('All characters are cleaned.')
            logging.info(out_path + ' is created')
            return refined_characters_df
        except ValueError:
            logging.error(ValueError)

    def get_comics(self, offset=0, limit=100):
        """ Call Comics endpoints to get comics
        :param offset:  the position in the dataset of a particular record
        :param limit: The maximum number of entries to return.
        :return: API Response
        """
        try:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')
            params = {'ts': timestamp,
                      'apikey': self.public_key,
                      'hash': self.hash_params(timestamp),
                      'limit': limit,
                      'offset': offset}

            response = requests.get('https://gateway.marvel.com/v1/public/comics',
                               params=params)

            results = response.json()
            return results
        except ValueError:
            logging.error(ValueError)

    def get_all_comics(self, out_path):
        """ Get all comics from Marvel API
        :param out_path: Location to save comics dataset
        """
        curr_offset = 0
        curr_limit = 100
        total_cnt_comics = 0

        try:
            logging.info('Pulling comics data is started..')
            print('Pulling comics data is started..')
            while curr_offset <= total_cnt_comics:
                time.sleep(10)
                if total_cnt_comics == 0:
                    raw_data = self.get_comics(curr_offset, curr_limit)
                    total_cnt_comics = raw_data['data']['total']
                    curr_offset += 100
                else:
                    raw_data = self.get_comics(curr_offset, curr_limit)
                    curr_offset += 100

                current_ts = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
                path = out_path + 'comics_' + str(curr_offset) + '_' + current_ts + '.json'
                df = pd.DataFrame(dict(raw_data)['data']['results'])
                df['current_timestamp'] = current_ts

                df.to_json(path)
                print(path, 'is created')
                logging.info(path + ' is created')

            logging.info('Pulling comics data is finished.')
            print('Pulling comics data is finished.')
        except ValueError:
            logging.error(ValueError)

    def comics_cleansing(self, in_path, out_path):
        """ Clean all raw comics data
        :param in_path: Location to read all saved comics raw data
        :param out_path: Location to save cleaned comics data
        :return: Cleaned comics pandas dataframe
        """
        path_to_json = in_path
        json_files = glob.glob(os.path.join(path_to_json, '*.json'))

        comics_df = pd.concat((pd.read_json(f) for f in json_files))
        try:
            logging.info('Comics cleansing started..')
            print('Comics cleansing started..')
            comics_df['characters_id_involved'] = comics_df['characters'].apply(
                lambda x: [ids['resourceURI'].split('/')[-1] for ids in dict(x)['items']]
                if dict(x)['available'] != 0 else False)

            comics_df = comics_df[comics_df['characters_id_involved'] != False]
            refined_comics_df = comics_df[['id', 'title', 'characters_id_involved']].explode(
                'characters_id_involved').reset_index(drop=True)
            refined_comics_df['characters_id_involved'] = refined_comics_df['characters_id_involved'].astype(int)
            refined_comics_df.to_csv(out_path + 'cleaned_comics.csv')

            logging.info('All comics are cleaned.')
            print('All comics are cleaned.')
            logging.info(out_path + ' is created')

            return refined_comics_df
        except ValueError:
            logging.error(ValueError)

    def data_aggregation(self, characters_df, comics_df, out_path):
        """ Combine characters and comics dataframe and apply some data aggregation
        :param characters_df: Cleaned character pandas dataframe
        :param comics_df: Cleaned comics pandas dataframe
        :param out_path: Location to save aggregated data
        """
        try:
            logging.info('Data aggregation is started..')
            print('Data aggregation is started..')

            final_df = characters_df.merge(comics_df, left_on='id', right_on='characters_id_involved', how='inner')

            final_result = final_df.groupby(['name', 'id_x', 'comics_count'])['title'].count().reset_index().rename(
                columns={'id_x': 'character_id', 'comics_count': 'count_pulled', 'title': 'count_calculated'})

            final_result.to_csv(out_path + 'final_results.csv')

            final_result['difference'] = final_result['count_pulled'] - final_result['count_calculated']
            verified_df = final_result[final_result['difference'] > 0]
            verified_df.to_csv(out_path + 'verified_results.csv')

            logging.info('Data aggregation is finished')
            print('Data aggregation is finished.')
            logging.info(out_path + ' is created')

            return final_result
        except ValueError:
            logging.error(ValueError)

    def run(self):
        """ Organize the execution of the workflow
        """
        try:
            path_map = self.create_directory()
            logging.basicConfig(filename=path_map['logging']['log'] + 'app.log', filemode='w',
                                format='%(asctime)s - %(message)s',
                                level=logging.INFO)
            logging.info('App Started..')

            # self.get_all_characters(path_map['characters']['raw'])
            refined_characters_df = self.characters_cleansing(path_map['characters']['raw'],
                                                         path_map['characters']['stage'])

            # self.get_all_comics(path_map['comics']['raw'])
            refined_comics_df = self.comics_cleansing(path_map['comics']['raw'],
                                                      path_map['comics']['stage'])

            final = self.data_aggregation(refined_characters_df, refined_comics_df, path_map['final']['curated'])
            print('Here\'s the final results:\n', final)
        except ValueError:
            logging.error(ValueError)
