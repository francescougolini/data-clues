# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

__all__ = ['KeywordsMatcher']

import re
import pandas
from data_clues.utilities import basic_unique_values


@pandas.api.extensions.register_dataframe_accessor("dc_matching")
class KeywordsMatcher(object):
    ''' Match a dataframe to a given set of reference lists.

    Attributes:
        pandas_obj: A Pandas object containing the data to be processed.
    '''

    # Constructor
    def __init__(self, pandas_obj):

        # TODO: add validator self._validate(pandas_obj)
        self._dataframe_obj = pandas_obj

    def filter_column_by_keywords(self, target_column_label=None, reference_keywords_list=None):
        '''Filter a specific column of the target_df according to a reference list.

        Args:
            target_column_label: The label of the column to be inspected.
            reference_keywords_list: The list of keywords used to filter the target_column_label.
        Raises:
            None
        Returns:
            A list with the filtered values.
        '''

        _target_keywords_list = list(self._dataframe_obj[target_column_label])

        # Concatenate all the string and regular expression and compile them
        _reference_keywords_regex = re.compile(
            '|'.join(reference_keywords_list)
        )

        # Generate a list with all the dossiers with bad keywords
        _filtered_list = list(filter(lambda target_keyword: re.match(
            _reference_keywords_regex, target_keyword), _target_keywords_list)
        )

        return _filtered_list

    def match_rows_to_keywords(self, target_column_label=None, reference_keywords_list=None, results_column_label=None):
        ''' Match the targetted values Series to a given list and append the results to a new Series in the target_df.

        The matching values can be: 
        - 0, if the value matches an elment in the reference_keyword_list; 
        - 1, if the value does NOT match any of the elments in the reference_keyword_list

        Args: 
            target_column_label: The name of the column to be analysed.
            reference_keywords_list: The list of keywords used to filter the object_series.
            results_column_label: The name of the new column populated with the result of the matching process.
        Raises: 
            AttributeError: If any of the attribute is not provided. 
        Returns:
            None
        '''

        if all(element is not None for element in [target_column_label, reference_keywords_list, results_column_label]):

            # Concatenate all the string and regular expression and compile them
            _reference_keywords_regex = re.compile(
                '|'.join(reference_keywords_list))

            # Collect all the unique values in an array and store them in a new DataFrame
            _unique_values = pandas.Series(basic_unique_values(
                self._dataframe_obj[target_column_label]))

            _matching_results_series = _unique_values.str.match(
                _reference_keywords_regex, case=True, flags=0, na='-')

            _unique_values_df = pandas.DataFrame({
                target_column_label: _unique_values,
                results_column_label: _matching_results_series
            })

            '''
            # Create a new column and, for each row, return the matching result
            #self._dataframe_obj[results_column_label] = ''

            for index, item in _unique_values_df.iterrows():

                for index, row in self._dataframe_obj[self._dataframe_obj[target_column_label] == item[target_column_label]].iterrows():

                    self._dataframe_obj.at[index, results_column_label] = item[results_column_label]

            '''

            self._dataframe_obj = self._dataframe_obj.merge(
                _unique_values_df, how='outer', on=target_column_label)

        else:

            raise AttributeError(
                'Missing attributes for method match_rows_to_keywords (KeywordsMatcher).')

    def _data_matching_iterator(self, keywords_parameters_dicts, index):
        ''' Run the rows-to-keywords matching concurrently, with each process responsible for a keywords_parameters_dicts dictionary (identified by the index). 

        Args: 
            keywords_parameters_dicts: A list of dictionaries containing the parameters to be passed to the the match_rows_to_keyword function.
            index: The element in the keywords_parameters_dicts to be used.
        Raises: 
            None
        Returns:
            The processed dataframe with a new column containing the result of the matching. 
        '''

        self.match_rows_to_keywords(**keywords_parameters_dicts[index])

        return self._dataframe_obj

    def bulk_data_matching(self, keywords_parameters_dicts):
        '''For each dictionary of keyword arguments, run in pararallel the rows-keywords matching function. 

        Args: 
            keywords_parameters_dicts: A list of dictionaries containing the parameters to be passed to the the match_rows_to_keyword function. 
        Raises: 
            None
        Returns:
            The processed Pandas dataframe with new columns containing the global (i.e. from all the processes) results of the matching operations.  
        '''

        from functools import partial
        from rosetta.parallel.parallel_easy import imap_easy

        _list_length = len(keywords_parameters_dicts)

        _data_matching_partial_func = partial(
            self._data_matching_iterator, keywords_parameters_dicts)

        _results_iterator = imap_easy(
            _data_matching_partial_func, range(_list_length), -1, 10000)

        for result in _results_iterator:

            self._dataframe_obj = result

        return self._dataframe_obj

    def get_dataframe(self):
        ''' Return the processed dataframe.

        Args: 
            None
        Raises: 
            None
        Returns:
            The targeted dataframe.  
        '''

        return self._dataframe_obj
