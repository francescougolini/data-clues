# -*- coding: utf-8 -*-
# Copyright (c) 2021 Francesco Ugolini

import json

__all__ = ['generic_hostnames', 'popular_urls', 'placeholder_names']

# Source

# References

# A. Regular Expressions

# A.1 - Generic TLDs
generic_tlds = (
    r'(com|org|net|biz|info)'
)

# B. JSON Files

# B.1 - Popular URLs
popular_urls = []

with open('data/input/popular_urls.json', 'r') as urls:
    popular_urls = json.load(urls)

# C. Coded lists

# C.1 - Placeholder names

placeholder_names = ['John Doe', 'Jane Doe', 'Mary Moe', 'Jean Dupont',
                     'Max Mustermann', 'Erika Mustermann', 'João das Couves', "Maria das Couves"]
