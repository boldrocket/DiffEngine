__author__ = 'Ammar Akhtar'

"""
Contains the DiffEngine class, which can be run from the command line

The DiffEngine takes an input list of strings which it compares
to a seed list of potentially similar strings and outputs the 'most different' values from the input.

Useful to sift thru large data sets when trying to build a meaningful set of training data

See class documentation for more information

"""


import os, mmap
import glob
import shutil
import re
import csv
import numpy as np


from loggers import ch, stream_logger, main_logger, iologger, classifier_logger, final_stats
from regex_filters import *


# list of fields in the input data #todo - update this for your own data!
HEADERS = "entry_date|TRANS_TYPE|COMPARISON_FIELD|SYS_ID|GEN_DATE|TRANS_system|TRANS_amount|acc_bal|_NAME_"

#fields we want in the input data #todo - update this for your own data!
WANTED = "entry_date|COMPARISON_FIELD|TRANS_system|TRANS_amount"

#fields containing useful strings or tokens to compare
NARRS = "COMPARISON_FIELD|_NAME_"

#fields which will be pushed into the output file #todo - update this for your own data!
AFTER_CLEAN = "COMPARISON_FIELD|TRANS_amount|TRANS_system|_NAME_|entry_date"

HEADERS = HEADERS.split('|')
WANTED = WANTED.split('|')
NARRS = NARRS.split('|')
AFTER_CLEAN = AFTER_CLEAN.split('|')

# indices from headers which we care about in the comparison later #todo - update this for your own data!
idx_TRANS_amount = HEADERS.index('TRANS_amount')
idx_transaction_date = HEADERS.index('entry_date')
idx_TRANS_NARR = HEADERS.index('COMPARISON_FIELD')
li_idx_narrs = [HEADERS.index(x) for x in NARRS[1:]]

# used to
MATCH_THRESHOLD = 0.6


def current_path():
    curr_path = os.path.dirname(os.path.realpath(__file__))

    return curr_path


class DiffEngine(object):
    """
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


    """
    def __init__(self, **kwargs):
        main_logger.info('*****NEW PROCESS STARTING')
        # get fileloc from kwargs or current_path()
        self.fileloc = kwargs.get('fileloc', current_path())

        self.data_ext = kwargs.get('ext', '*.dat')
        self.data_file_path = os.path.join(self.fileloc, self.data_ext)
        self.outfile = kwargs.get('outfile', 'out/outfile0.out')

        self.processed_rows = 0
        self.skipped_rows = 0
        self.outfile_ctr = 0
        self.delimiter = '|'
        self.seed_files = os.path.join(current_path(), '*.seed')

        main_logger.info('loading %s' % self.seed_files)
        #warning only supports a single .seed file currently
        for c, dat_file in enumerate(glob.iglob(os.path.join(self.seed_files))):
            self.load_seed_data(dat_file)  #creates self.seed

        self.already_checked = set()
        self.density_fail = set()

        seed_as_a_list = [x for x in ' '.join(self.seed['t_desc_clean']).split(' ') if len(x) > 2]

        self.re_seed_as_a_list = []
        for x in xrange(len(self.seed['t_desc_clean'])):
            self.re_seed_as_a_list.append(r"("+')|('.join(list(self.seed['t_desc_clean'])[x:x+80])+")")
            x+=80

        main_logger.info('creating seed stats')
        self.seed_density = {str(x): seed_as_a_list.count(x) for x in seed_as_a_list}
        self.seed_stats = {}
        self.build_seed_stats()
        main_logger.info('completed seed stats')
        self.rank_files()
        self.current_dat_file = None  # used in the load process


    def build_seed_stats(self):
        self.seed_stats['mean'] = np.average(self.seed_density.values())
        self.seed_stats['stddev'] = np.std(self.seed_density.values())
        self.seed_stats['densities'] = sorted(set(self.seed_density.values()), reverse=True)

        self.lower_gap = max(0.25, (self.skipped_rows)/(1 + self.processed_rows))

        self.seed_stats['lower_threshold'] = np.ceil(self.seed_stats['mean']/10)
        self.seed_stats['upper_threshold'] = self.seed_stats['mean'] + 10 * self.seed_stats['stddev']

        classifier_logger.info("THRESHOLDS: %s\t%s\t%s"%(self.seed_stats['lower_threshold'],
                                                         self.seed_stats['mean'],
                                                         self.seed_stats['upper_threshold']))

        final_stats.info("THRESHOLDS: %s\t%s\t%s"%(self.seed_stats['lower_threshold'],
                                                         self.seed_stats['mean'],
                                                         self.seed_stats['upper_threshold']))
        # print self.seed['t_desc_clean']
        pass

    def dataAsTableFromDict(self, dataAsDict):
        return zip(*list([[k] + dataAsDict[k] for k in dataAsDict]))


    def dataAsDictFromTable(self, dataAsTable):
        return {k[0]: list(k[1:]) for k in zip(*dataAsTable)}

    def load_seed_data(self, seed_file):
        #todo - make this specific to your own seed data!
        heads = "t_desc|tag_name|counter_party|avg".split('|')
        starter = 1
        Wants = heads

        try:
            fileHandle = open(seed_file, 'r')
        except IOError as e:
            main_logger.error(e)
            return e

        rows = [[x.strip() for x in l.split(self.delimiter)] for l in fileHandle]

        fileHandle.close()

        if len(rows[-1]) != len(heads):
            rows = rows[:-1]

        self.seed = self.dataAsDictFromTable(rows)

        self.seed['t_desc_clean'] = set()
        for x in self.seed['t_desc']: # todo: update the 't_desc' key with the field name from your own narrative
            x2 = [y.strip() for y in x.strip().split(' ')]
            # l = [item for item in x2 if not re.match(re_bad_seed,item)]
            l = []
            for item in x2:
                if re.match(re_bad_seed, item):
                    continue
                else:
                    l.append(item)
            self.seed['t_desc_clean'].add(' '.join(l))


    def xsvParser(self, fileLoc):
        """
        loads delimited files to dictionaries

        args:
            fileLoc(str): full path to delimited data file - include the file itself


        Output:
            single_series_flat == false: dataAsDict(dict) = { row or col header : [ series in row or col ] }

        """

        try:
            fileHandle = open(fileLoc, 'r')
        except IOError as e:
            main_logger.error(e)
            return e

        # todo: the conversion of '|?' to '|' was a quirk of the dataset I was using.
        # Feel free to remove this if not useful

        rows = [[x.strip() for x in l.replace('|?', '|').split(self.delimiter)] for l in fileHandle]
        fileHandle.close()
        rows = self.process_data(rows)

        return rows


    def process_data(self, all_rows):

        dat = [HEADERS]

        for row_ctr, row_data in enumerate(all_rows):
            self.processed_rows +=1

            if len(row_data) < len(HEADERS):
                main_logger.debug('row %s misaligned to headers'%(row_ctr))
                self.skipped_rows +=1
                continue  # bad row


            try:
                row_trans_amount = float(row_data[idx_TRANS_amount])
            except ValueError, e:
                main_logger.debug('row %s misaligned to headers, %s'%(row_ctr, e))
                self.skipped_rows +=1
                continue

            if row_trans_amount > 0:
                self.skipped_rows +=1
                continue

            if int(row_data[idx_transaction_date].split('/')[0]) < 2011: # exclude row if pre 2011
                self.skipped_rows +=1
                continue


            if row_data[idx_TRANS_NARR] == '' or row_data[idx_TRANS_NARR] == " " or len(row_data[idx_TRANS_NARR]) < 2:
                self.skipped_rows +=1
                continue

            row_data[idx_TRANS_NARR] = re.sub(r'\s{2,}', ' ', row_data[idx_TRANS_NARR])

            if re_bad_narratives.match(row_data[idx_TRANS_NARR]) or re_dead_narratives.match(row_data[idx_TRANS_NARR]):
                self.skipped_rows +=1
                continue

            if not self.pick_or_reject_narrative(row_data[idx_TRANS_NARR]):
                self.skipped_rows +=1
                continue

            dat.append(row_data)

        dat = self.dataAsDictFromTable(dat)

        for k in dat.keys():
            if k not in AFTER_CLEAN: del dat[k]

        dat = self.dataAsTableFromDict(dat)

        return dat

    def pick_or_reject_narrative(self, comparator):
        # list_of_things_to_find = []
        # for comparator in list_of_things_to_find:


        density_check_failed = False
        comparator_tokens_with_bad_density = 0.
        density_threshold = 0.4

        # easy checks
        if len(comparator) < 3:
            classifier_logger.debug('comparator %s rejected - too short '%comparator)
            return False

        if 'SOME STRING' in comparator or 'ANOTHER STRING' in comparator:
            classifier_logger.debug('comparator %s rejected'%comparator)
            return False

        if comparator in self.already_checked:
            classifier_logger.debug('comparator %s already checked'%comparator)
            return False

        if comparator in self.seed['t_desc_clean']:
            classifier_logger.debug('comparator %s in seed tokens'%comparator)
            return False
        # end easy checks

        classifier_logger.info("STARTING comparator %s"%comparator)

        # check comparator is 'different' enough from other stuff we have
        for c in comparator.strip().split(' '):

            if density_check_failed:
                self.already_checked.add(comparator)
                self.density_fail.add(comparator)
                return False

            if len(c) < 1:
                density_threshold *= 0.75
                continue

            # if comparator words are in re_bad_* lists, then lower density threshold and move to next word
            # if re.match(re_bad_seed, c) or re.match(re_bad_narratives, c):
            if re_bad_seed.match(c) or re_bad_narratives.match(c):
                density_threshold *= 0.75
                continue

            if len(c) < 4:
                density_threshold *= 0.75

            if re_dead_narratives.match(c):
                density_threshold *= 0.3

            classifier_logger.info("current density of token %s:\t%s"%(c, self.seed_density.get(c,0)))
            # check if eligible tokens in comparator are in the unwanted density range
            if self.seed_stats['lower_threshold'] < self.seed_density.get(c, 0) < self.seed_stats['upper_threshold']:

                comparator_tokens_with_bad_density += 1

                # only check if something has changed
                if (comparator_tokens_with_bad_density / len(comparator.strip().split(' '))) >= density_threshold:
                    # fails if too many token in the comparator have unwanted density
                    density_check_failed = True
                    classifier_logger.warning('comparator %s failed density check on %s'%(comparator, c))



        # now worth building the modified comparator
        modified_comparator = [c for c in comparator.strip().split(' ') if
                               (len(c) > 0 and not re.match(re_bad_seed, c))]

        classifier_logger.debug('modified comparator %s created for comparator %s'%(modified_comparator, comparator))

        # easy checks on modified comparator
        if ' '.join(modified_comparator) in self.already_checked:
            classifier_logger.debug('modified comparator already checked for comparator %s'%comparator)
            return False

        if ' '.join(modified_comparator) in self.seed['t_desc_clean']:
            classifier_logger.debug('modified comparator in seed tokens for comparator %s'%comparator)
            return False

        # check if comparator is in any item in seed_list
        for seed_item in self.seed['t_desc_clean']:
            comparator_coverage_for_seed_list_item = self.get_comparator_coverage_for_seed_list_item(
                seed_item, modified_comparator)

            if comparator_coverage_for_seed_list_item is False:  # bad seed_item
                continue

            if comparator_coverage_for_seed_list_item > MATCH_THRESHOLD:
                self.already_checked.add(' '.join(modified_comparator))
                self.already_checked.add(comparator)
                str_ = "comparator_coverage_for_seed_list_item > MATCH_THRESHOLD"
                flt_ = str(comparator_coverage_for_seed_list_item)
                flt_ = flt_[:min(5,len(flt_))]
                classifier_logger.debug('%s = %s for comparator %s on seed_item %s'%(str_, flt_, comparator, seed_item))
                return False

        # if all tests fail, then this is a genuinely new thing and we should keep it
        self.already_checked.add(comparator)
        self.already_checked.add(' '.join(modified_comparator))

        # add it to the seed list
        for mc in modified_comparator:
            self.seed_density[mc] = self.seed_density.get(mc, 0) + 1
        classifier_logger.critical('SUCCESS on comparator %s with modified_comarator %s'%(comparator, modified_comparator))
        self.build_seed_stats()
        return True


    def get_comparator_coverage_for_seed_list_item(self, item, comparator):
        item_tokens = item.strip().split(' ')

        item_tokens_in_comparator = [i for i in item_tokens if i in comparator]

        try:
            r = float(len(item_tokens_in_comparator)) / len(item_tokens)
            return r
        except ZeroDivisionError, e:
            classifier_logger.debug('zero division error on seed list item %s'%item)
            return False


    def set_outfile(self):
        # get outfile
        outfile = self.outfile
        outfileLoc = os.path.join(current_path(), outfile)
        incl_headers = False

        try:
            outfile_size = os.stat(outfileLoc).st_size
        except:
            # this is the first time thru
            iologger.info('creating first outfile')
            outfile_size = 0
            incl_headers = True

        if outfile_size > 2000000:
            self.outfile_ctr += 1
            iologger.info('rotating outfile to %s' % self.outfile_ctr)
            self.outfile = outfile.split('.')[0] + str(self.outfile_ctr) + '.out'
            incl_headers = True

        return self.outfile, incl_headers

    def write_pipe_delimited(self, write_file, rows_to_write, headings=False):

        if headings is False:
            out_file = csv.writer(open(write_file, 'a'), delimiter="|")

        if headings is True:
            out_file = csv.writer(open(write_file, 'w'), delimiter="|")
            try:
                out_file.writerow(rows_to_write[0])
            except IOError, e:
                iologger(e)
                iologger(self.current_dat_file)
                return False

        try:
            out_file.writerows(rows_to_write[1:])
        except IOError, e:
            iologger(e)
            iologger(self.current_dat_file)
            return False

        return True

    def rank_files(self):
        # todo: scan file for correct date range
        # add method invoked from the __init__ which:
        # parses thru all the datafiles in reverse size order, and
        # builds list of lists such as:
        # [filename, count of date match, count of re_bad_narratives instances, count of self.seed['t_desc_clean'] instances, figure for comparison]
        # where "figure for comparison" is compounded from the 3 previous values
        # the list of lists should then be sorted descending based on "figure for comparison"; and
        # the normal process should resume
        main_logger.info('running rank_files')
        outer_list = []

        for c, dat_file in enumerate(glob.iglob(self.data_file_path)):
            if c % 100 == 0: stream_logger.info('%s\t%s'%(c, len(outer_list)))

            m1 = 0.
            m2 = 0.
            inner_list = {'dat_file':dat_file}

            size = os.stat(dat_file).st_size
            f = open(dat_file)
            data = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            f.close()

            m1 += len(re.findall(re_date_search, data))
            # data.seek(0)
            data.close()

            if m1 != 0:
                pass
            #     m2 = len(re_bad_narratives.findall(data))
            #     # m2 = len(re.findall(re_bad_narratives, data))
            else:
                continue
            # m2 += max(1,m2)

            inner_list.update({'date_match':m1})
            # inner_list.update({'bad_narr_match':m2})

            # too slow!!!
            # m3 = 0
            # for re_seed_as_a_list in self.re_seed_as_a_list:
            #     m3 += len(re.findall(re_seed_as_a_list, data))

            # inner_list.update({'seed_match':m3})
            inner_list.update({'score': m1})
            # inner_list.update({'score': m1/m2})
            outer_list.append(inner_list)

        outer_list = sorted(outer_list, key=lambda x:x['score'], reverse=True)
        self.ranked_files = outer_list
        main_logger.info('finished rank_files')
        pass



    def get_files(self):

        # for c, dat_file in enumerate(glob.iglob(self.data_file_path)):
        for c, ranked_entry in enumerate(self.ranked_files):

            dat_file = ranked_entry['dat_file']
            score_ = ranked_entry['score']

            main_logger.info('running %s with score %s' % (dat_file, score_))

            out = self.set_outfile()

            self.current_dat_file = dat_file  # used for logging

            dat = self.xsvParser(dat_file)

            main_logger.debug('parse complete, starting write')
            # dat = self.clean_dat(dat)
            # logger.info('clean complete, starting write')
            if len(dat) > 1:
                self.write_pipe_delimited(out[0], dat, out[1])
            main_logger.debug('clean complete, write complete')
            main_logger.info("after current file: %s processed; %s skipped; %s" % (self.processed_rows,
                                                                self.skipped_rows,
                                                                str(1. - (float(self.skipped_rows)/self.processed_rows))))

        self.current_dat_file = None #reinitialise

        main_logger.info("completed with %s processed; %s skipped; %s" % (self.processed_rows,
                                                                self.skipped_rows,
                                                                str(1. - (float(self.skipped_rows)/self.processed_rows))))
        final_stats.info('DENSITIES')

        for k,v in self.seed_stats.items():
            final_stats.info("%s\t%s"%(k,v))

        for k, v in sorted(self.seed_density.items(),key=lambda x: x[0]):
            final_stats.info("%s\t%s"%(k, v))


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='a test for passing arguments')

    parser.add_argument('-f', '--fileloc', required=False,
                        help='data file location; defaults to dat directory in current folder',
                        default=os.path.join(current_path(), 'dat'))

    parser.add_argument('-e', '--ext', required=False,
                        help='data file extension; defaults to *.txt',
                        default='*.txt')

    parser.add_argument('-o', '--outfile', required=False,
                        help='output file naming convention; defaults to out/outfile0.out',
                        default='out/outfile0.out')

    args = parser.parse_args()

    kwargs = vars(args)

    if not os.path.isabs(kwargs['fileloc']):
        kwargs['fileloc'] = os.path.join(current_path(), kwargs['fileloc'])
        main_logger.info('relative path provided, updating to absolute: %s' % kwargs['fileloc'])

    if not os.path.isdir(kwargs['fileloc']):
        main_logger.warning('fileloc is not a valid dir. using file location')
        kwargs['fileloc'] = os.path.dirname(os.path.realpath(__file__))

    print 'args', vars(args)

    worker = DiffEngine(**kwargs)
    worker.get_files()
