# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

__all__ = ['DataImporter']

import pandas
import sqlalchemy


class DataImporter:
    ''' Import data from a specified source and convert it in a Pandas Dataframe. 

    Attributes:
        csv_filepath: The path to the CSV file containing the targeted dataset. To be specified along with csv_filename. 
        csv_filename: The name of the CSV file containing the targeted dataset. To be specified along with csv_filepath. 
        db_type: The type of the database, see https://docs.sqlalchemy.org/en/13/core/engines.html for more details. 
        db_host: The hostname of the database.
        db_port: The port of the database. 
        db_name: The name of the database.
        table_name: The name of the table from which data have to be taken. 
        username: The username to access the database. 
        password: The password to access the database. 
    '''

    def __init__(self, csv_filepath=None, csv_filename=None, db_type=None, db_host=None, db_port=None, db_name=None, table_name=None, username=None, password=None):  # sql_query=None

        # Initialise the dataframe variable
        self._target_df = None

        import os

        if not os.path.exists('data'):
            os.mkdir('data')

        if all(element is not None for element in [db_host, db_port, db_name, table_name, username, password]):
            # Populated the dataframe from the data selected from the database

            # PostgreSQL is the default database engine. Always use the SqlAlchemy naming.
            db_type = db_type if db_type else 'postgresql'
            #_sql_query= sql_query if sql_query is not None else 'select * from ' + str(table_name)

            # Query the database to get the data to be processed and return a dataframe with these data

            _engine = sqlalchemy.create_engine(
                db_type + '://' + username + ':' + password +
                '@' + db_host + ':' + db_port + '/' + db_name
            )

            self._target_df = pandas.read_sql_table(table_name, _engine)

        elif all(element is not None for element in [csv_filepath, csv_filepath]):
            # Populated the dataframe from the data selected from the csv file

            # Build the path to open the CSV file
            from pathlib import Path

            _full_path = Path(csv_filepath) / csv_filename

            try:

                with open(_full_path) as _csv_file:

                    self._target_df = pandas.read_csv(_csv_file)

            except IOError as error:

                print(
                    "I/O error({0}): {1}".format(error.errno, error.strerror))

        else:

            raise AttributeError(
                'Missing class attributes for KeywordsMatcher.')

    def get_dataframe(self):
        ''' Return the retrieved dataframe.

        Args: 
            None
        Raises: 
            None
        Returns:
            The dataframe to be processed.  
        '''

        return self._target_df
