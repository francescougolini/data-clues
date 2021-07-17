# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

import pandas
import Levenshtein
from data_clues.utilities import advanced_unique_values

__all__ = ['SimilarityChecker']


@pandas.api.extensions.register_dataframe_accessor("dc_similarity")
class SimilarityChecker(object):
    ''' Check the similarity between pre-defined columns of a given dataframe using Levenshtein distance. 

    Attributes:
        pandas_obj: A Pandas object containing the data to be processed.
    '''

    def __init__(self, pandas_obj):

        # TODO: add validator self._validate(pandas_obj)
        self._dataframe_obj = pandas_obj

    def check_similarity(self, target_column_a_label=None, target_column_b_label=None, results_column_label=None):
        ''' Determine the similarity between two given Pandas Series. 

        Args:  
            target_column_a_labels: The label of one of the two Pandas Series to be proccessed. 
            target_column_b_labels: The label of one of the two Pandas Series to be proccessed. 
            results_column_labels: The label of the Pandas Series used to store the result from the similary check.
        Raises: 
            None
        Return: 
            None
        '''

        # Get the rows with unique values
        # TODO: put this operation in the brader bulk_similarity_check
        _unique_values_df = advanced_unique_values(
            self._dataframe_obj, target_column_a_label, target_column_b_label)

        _similarity_results_series = _unique_values_df.fillna('').apply(lambda row: Levenshtein.ratio(
            str(row[target_column_a_label]), str(row[target_column_b_label])), axis=1)

        _similarity_check_df = pandas.DataFrame({
            target_column_a_label: _unique_values_df[target_column_a_label],
            target_column_b_label: _unique_values_df[target_column_b_label],
            results_column_label: _similarity_results_series
        })

        self._dataframe_obj = self._dataframe_obj.merge(
            _similarity_check_df, how='outer', on=(target_column_a_label, target_column_b_label)
        )

    def _similarity_check_iterator(self, similarity_parameters_dicts, index):
        '''Run the similarity check concurrently,  with each process responsible for a similarity_parameters_dicts 
            dictionary (identified by the index). 

        Args: 
            similarity_parameters_dicts: A list of dictionaries containing the parameters to be passed to the 
                _similarity_check_function function.
            index: The element in the similarity_parameters_dicts to be used.
        Raises: 
            None
        Returns:
            The processed dataframe with a new column containing the result of the similarity check. 
        '''

        self.check_similarity(**similarity_parameters_dicts[index])

        return self._dataframe_obj

    def bulk_check_similarity(self, similarity_parameters_dicts):
        '''For each dictionary of keyword arguments, run in pararallel the check_similarity function. 

        Args: 
            similarity_parameters_dicts: A list of dictionaries containing the parameters to be passed to the 
                check_similarity function. 
        Raises: 
            None
        Returns:
            The processed Pandas dataframe with new columns containing the results of the similarity checks. 
        '''

        from functools import partial
        from rosetta.parallel.parallel_easy import imap_easy

        _list_length = len(similarity_parameters_dicts)

        _similarity_check_partial_func = partial(
            self._similarity_check_iterator, similarity_parameters_dicts)

        _results_iterator = imap_easy(
            _similarity_check_partial_func, range(_list_length), -1, 10000
        )

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
