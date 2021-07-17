data_clues
========================

Utility to spot similarities or patterns in a dataset.

**Warning:** the library is __NOT__ maintained, and it's __NOT__ intended for production.

The tool is designed to process a large set of data with little or no insight on it, except for clues or known patterns. This utility allows to check datapoints against each other in order to find similarities and/or differences.

In order to do that, three types of actions have been included so far:
- keywords match;
- similarity check;
- occurrence analysis (numbers, words, and other characters).

This utility provides new clues and insights on the dataset. The ability of the analysts to spot them is paramount, as the utility is __NOT__ intended to replace them.

### Notes

Although the utility is currently **unmaintained**, there are two other actions worth to be considered: common patterns identification and frequency measurement. 

A **made-up** example is included in main.py, which is the entry point to begin using the tool. Mock data are also included, check [data/input](data/input) (data randomly generated using Mockaroo). The utilities were designed to also allow data retrieval from a database.

:warning: Again, this is an unmaintained project, and it's __NOT__ intended for production. Things might not work and data could get lost. Therefore, ensure you have backed up everything before testing this utility. Do __NOT__ use the utility in a production environment.

## Requirements: 

    - pandas 
    - sqlalchemy
    - pyarrow
    - nltk
    - scipy
    - rosetta
    - python-Levenshtein

## Methods

### Keywords Matcher

    - filter_column_by_keywords(target_series_header='', reference_keywords_list='')
    - match_rows_to_keywords(target_series_header='', reference_keywords_list='', results_series_header='')
    - bulk_data_matching(keywords_parameters_list)
    - get_dataframe()

### Similarity Checker

    - check_similarity(target_series_a_header='', target_series_b_header='', results_series_header='') 
    - bulk_check_similarity(similarity_parameters_list)
    - get_dataframe()

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Copyright Notice

Copyright (c) 2021 Francesco Ugolini
