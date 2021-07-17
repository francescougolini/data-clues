# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini <contact@francescougolini.com>

__all__ = ['basic_unique_values', 'advanced_unique_values']


def basic_unique_values(target_series):
    ''' Get unique values from a Pandas Series. 

    Args: 
        target_series: The Pandas Series to be proccessed.  
    Raises: 
        None
    Return: 
        A list with unique values.
    '''

    _unique_values = target_series.unique()

    return _unique_values


def advanced_unique_values(target_dataframe, *target_series_headers):
    ''' Get unique values from multiple Pandas Series. 

    Args: 
        target_dataframe: The Pandas Dataframe from which the series belong. 
        target_series_headers: The headers of Pandas Series to be proccessed.  
    Raises: 
        None
    Return: 
        A list with unique values.
    '''

    _series_subset = [series for series in target_series_headers]

    _unique_values = target_dataframe.drop_duplicates(subset=_series_subset)

    return _unique_values
