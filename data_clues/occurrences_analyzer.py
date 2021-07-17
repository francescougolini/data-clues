# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

__all__ = ['CharacterOccurrencesAnalyzer']

import pandas
from data_clues.utilities import basic_unique_values


@pandas.api.extensions.register_dataframe_accessor("dc_occurrences")
class CharacterOccurrencesAnalyzer(object):
    ''' Provide a numerical weighted description of the different character types (word, digit, sign) for given Series in the Dataframe.

    Attributes:
        pandas_obj: A Pandas object containing the data to be processed.

        self._word_factor: The weight of word characters. If not provided a default one will be used. 
        self._digit_factor: The weight of digit characters. If not provided a default one will be used. 
        self._sign_factor: The weight of sign characters. If not provided a default one will be used. 
    '''

    # Constructor
    def __init__(self, pandas_obj, word_factor=None, digit_factor=None, sign_factor=None):

        # TODO: add validator self._validate(pandas_obj)
        self._dataframe_obj = pandas_obj

        self._word_factor = word_factor if word_factor is not None else 1
        self._digit_factor = digit_factor if digit_factor is not None else 1
        self._sign_factor = sign_factor if sign_factor is not None else 2

    def _character_occurrences_ratio(self, target_string=None, custom_factors=None):
        ''' Calculate the occurrence ratio for a given string.

        Args:
            target_string: The string to be analysed.
            custom_factors: If provided, an array containing custom weighting factors for each character type,
                as [word_factor, digit factor, sign factor].
        Raises:
            None
        Returns:
            A floating point number, which represents the character occurrences ratio. 
        '''

        # Words (a-Z)
        _word_factor = custom_factors[0] if custom_factors is not None and custom_factors[0] is not None else self._word_factor

        # Digits (0-9)
        _digit_factor = custom_factors[1] if custom_factors is not None and custom_factors[
            1] is not None is not None else self._digit_factor

        # Other characters (-, +, ., etc.)
        _sign_factor = custom_factors[2] if custom_factors is not None and custom_factors[
            2] is not None is not None else self._sign_factor

        _digit_characters_count = len(
            list(filter(lambda character: character.isdigit(), str(target_string))))
        _word_characters_count = len(
            list(filter(lambda character: character.isalpha(), str(target_string))))
        _other_characters_count = len(
            target_string) - _digit_characters_count - _word_characters_count

        total_characters_count = len(list(target_string))

        occurrence_ratio = ((1 - (1 / _sign_factor)) + (_word_factor * _word_characters_count) + (_digit_characters_count**(
            1 - (_digit_factor * _digit_characters_count))) + ((1 / _sign_factor) * _other_characters_count)) / total_characters_count

        return float(occurrence_ratio)

    def _character_occurrences_analysis(self, target_column_label=None, custom_factors=None, results_column_label=None):
        ''' Measure the occurrence ratio of each element in a given Series and append the results in a new Series in the target_df.

        Args: 
            target_column_label: The name of the column to be analysed.
            custom_factors: An optional array containing numerical custom weights for the different character types, 
                as [word_factor, digit_factor, sign_factor].
            results_column_label: The name of the new column populated with the results of the analysis.
        Raises: 
            AttributeError: If any of the attribute is not provided. 
        Returns:
            None
        '''

        if all(element is not None for element in [target_column_label, results_column_label]):

            # Collect all the unique values in an array and store them in a new DataFrame
            _unique_values = pandas.Series(basic_unique_values(
                self._dataframe_obj[target_column_label]))

            _occurrence_results_series = _unique_values.apply(
                lambda row: self._character_occurrences_ratio(row, custom_factors) if row is not None else 0)

            _unique_values_df = pandas.DataFrame({
                target_column_label: _unique_values,
                results_column_label: _occurrence_results_series
            })

            self._dataframe_obj = self._dataframe_obj.merge(
                _unique_values_df, how='outer', on=target_column_label)

        else:

            raise AttributeError(
                'Missing attributes for _character_occurrences_analysis (CharacterOccurrencesAnalyzer).')

    def _character_occurrences_iterator(self, occurrences_parameters_dicts, index):
        ''' Run the types occurrences analysis concurrently, with each process responsible for a 
            occurrences_parameters_dicts dictionary (identified by the index).

        Args: 
            occurrences_parameters_dicts: A list of dictionaries containing the parameters to be passed to the 
                the _character_occurrences_analysis function.
            index: The element in the target_column_labels to be used.
        Raises: 
            None
        Returns:
            The processed dataframe with a new column containing the result of the character occurrences analysis. 
        '''

        self._character_occurrences_analysis(
            **occurrences_parameters_dicts[index])

        return self._dataframe_obj

    def bulk_character_occurrences_analysis(self, occurrences_parameters_dicts):
        '''For each dictionary of keyword arguments, run in parallel the _character_occurrences_analysis function. 

        Args: 
            occurrences_parameters_dicts: A list of dictionaries containing the parameters to be passed to the
                the _character_occurrences_analysis. 
        Raises: 
            None
        Returns:
            The processed Pandas dataframe with new columns containing the global (i.e. from all the processes) 
            results of the character occurrences analysis.  
        '''

        from functools import partial
        from rosetta.parallel.parallel_easy import imap_easy

        _list_length = len(occurrences_parameters_dicts)

        _character_occurrences_partial_func = partial(
            self._character_occurrences_iterator, occurrences_parameters_dicts)

        _results_iterator = imap_easy(
            _character_occurrences_partial_func, range(_list_length), -1, 10000)

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
