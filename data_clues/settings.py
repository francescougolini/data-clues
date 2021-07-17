# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

import json

__all__ = ['SettingsReader']


class SettingsReader():
    ''' Provide the module with the information necessary to access and process datasets.

    Attributes:
        None
    '''

    def __init__(self, config_filename='config.json', config_filepath=''):

        with open(config_filepath + config_filename) as settings:
            self._settings = json.load(settings)

    def get_source_data(self):
        ''' Provide the necessary data to retrive the dataset to be processed. 

        Args: 
            None
        Raises: 
            Exception: If the JSON config file is not properly configured. 
        Returns:
            A dictionary with the data necessary to retrive a dataset. 
        '''

        _settings = self._settings['settings']

        if _settings['data_source'] == 'database':

            _db_host = _settings['database']['host']
            _db_port = _settings['database']['port']
            _db_name = _settings['database']['name']
            _table_name = _settings['database']['table_name']
            _username = _settings['database']['username']
            _password = _settings['database']['password']

            _source_data_kwargs = {'db_host': _db_host, 'db_port': _db_port, 'db_name': _db_name,
                                   'table_name': _table_name, 'username': _username, 'password': _password}

        elif _settings['data_source'] == 'csv':

            _csv_filepath = _settings['csv']['filepath']
            _csv_filename = _settings['csv']['filename']

            _source_data_kwargs = {
                'csv_filepath': _csv_filepath, 'csv_filename': _csv_filename}

        else:

            raise Exception(
                f'Unable to retrive any data source, please check you have provided all the details in the settings file. (Ref. {self.__class__.__name__})')

        return _source_data_kwargs
