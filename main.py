#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini

# NOTE: This is just an examplificatory use of the utility. Edit the following code according
# to the desired use and datasets available.

import data_clues as dc

# 1) Import data references in order to match or find similarities in the data source.
from data.input import reference_keywords_lists as rkl

# 2) Retrieve the csv or database data source from the config file.
# NOTE: remember to specify in config.json the "data_source" type, i.e. "csv" or "database".
settings_reader = dc.SettingsReader('config.json')

# From the config gile retrive the source data to be processed.
data_importer = dc.DataImporter(**settings_reader.get_source_data())

target_df = data_importer.get_dataframe()

# A list of dictionaries containing the parameters to perform the matching operations.
matching_parameters_dict = [
    {
        'target_column_label': 'full_name',
        'reference_keywords_list': rkl.placeholder_names,
        'results_column_label': 'match_full_name'
    },
    {
        'target_column_label': 'email',
        'reference_keywords_list': rkl.popular_urls,
        'results_column_label': 'match_email_domain'
    },
    {
        'target_column_label': 'website',
        'reference_keywords_list': rkl.generic_tlds,
        'results_column_label': 'match_website_tld'
    },
]

# A list of dictionaries containing the parameters to perform the similarity checks.
similarity_parameters_dict = [
    {
        'target_column_a_label': 'username',
        'target_column_b_label': 'email',
        'results_column_label': 'similarity_username_email'
    },
    {
        'target_column_a_label': 'username',
        'target_column_b_label': 'website',
        'results_column_label': 'similarity_username_website'
    },
    {
        'target_column_a_label': 'full_name',
        'target_column_b_label': 'username',
        'results_column_label': 'similarity_full_name_username'
    },
]

# A list of dictionaries containing the parameters to perform the similarity checks.
occurences_parameters_dicts = [
    {
        'target_column_label': 'email',
        # Check bulk_character_occurrences_analysis() to read more about custom factors.
        'custom_factors': [1, 3, 2],
        'results_column_label':'tweaked_similarity_email'
    },
    {
        'target_column_label': 'website',
        'results_column_label': 'standard_similarity_website'
    },
]

# Run the matching, similarity, and occurrences checks.
target_df = target_df.dc_matching.bulk_data_matching(
    matching_parameters_dict
)

target_df = target_df.dc_similarity.bulk_check_similarity(
    similarity_parameters_dict
)

target_df = target_df.dc_occurrences.bulk_character_occurrences_analysis(
    occurences_parameters_dicts
)

target_df.to_csv('data/output/processed_dataframe.csv',
                 index=None, header=True)
