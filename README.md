# DiffEngine
The DiffEngine takes an input list of strings which it compares
to a seed list of potentially similar strings and outputs the 'most different' values from the input.

Useful to sift thru large data sets when trying to build a meaningful set of training data

Requirements to run with minimal config required shown below

The regex imported from 'regex_filters' is used to filter out bad data

Logging is employed throughout the process so that all results and internal calibrations can be traced

The seed data, other input data and output files are all pipe-delimited by default

The seed narrative list is tokenised; with bad rows identified and excluded using 're_bad_seed'
from the imported regex. All tokens have a density calculated and the input data is ultimately
compared against the seed token statistics.

The example given in this implementation also uses early filtration based on broader criteria eg
- data density within a date range, see the rank_files() method
- matches against bad narrative examples see process_data() or pick_or_reject_narrative()

If the early filtration is passed, then the narrative is tokenised and reviewed against:
 - the imported regex lists
 - seed tokens
 where at each stage the density of bad tokens in the current narrative is used as the pass / fail metric

once an input row is acceptable for use, it becomes part of the seed data.
ie - the seed statistics are kept up to date throughout the process so that a particular syntax is not
'over-sampled' from the input data

===

Requirements to run with minimal config required:
- ensure the loggers.py and regex_filters.py files are in the same location as the DiffEngine
[or update the imports]

- a seed data file should be in the same location as the DiffEngine file.
see the load_seed_data() method for more information on how this is used, and be mindful of the #todo's !

- The input data to compare against the seed should be in the 'dat' directory
in the same location as the DiffEngine file

- update HEADERS, WANTED, NARRS above, and ensure these changes are reflected above.
This should then ensure the process_data() method is looking in the right place

- update re_date_search with desired date ranges [used in the rank_files() method]. The example
provided implies we are only looking for dates with Jan 1 2011 and Dec 31 2015, in the YYYY/MM/DD syntax
