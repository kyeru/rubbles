import datetime
import math
import os
import pandas
import pandas_datareader.data as pd
from pandas import DataFrame, Timestamp
import matplotlib.pyplot as plt


#
# Global variables
#

error_codes = list()
date_begin = datetime.date(2014, 1, 1)
date_end = datetime.date.today()


#
# Data Setup
#

def read_codes(file_path):
    '''Extracts company codes (and names if any) from given file.
    '''
    codes = list()
    names = list()
    with open(file_path) as code_file:
        for line in code_file:
            codes.append(str(line.strip()))
            # tokens = line.split()
            # if len(tokens) >= 2:
            #     codes.append(str(tokens[0]))
            #     names.append(str(tokens[1]))
    return codes, names


def read_records(code, record_dir):
    '''Reads records of a company and return as a DataFrame object.'''
    record_file = record_dir + '/' + code + '.txt'
    last_update = date_begin
    if not os.path.exists(record_file):
        return None, last_update
    with open(record_file, 'r') as f:
        records = list()
        for line in f:
            records.append(line)
        data = DataFrame()
        if len(records) > 0:
            ymd = records[-1].split(',')[0].split('-')
            d = datetime.date(int(ymd[0]), int(ymd[1]), int(ymd[2]))
            d = d + datetime.timedelta(days = 1)
            last_update = datetime.date(d.year, d.month, d.day)
            f.seek(0)
            data = pandas.read_csv(f, index_col = 0)
            # data.index = pandas.to_datetime(data.index)
        return data, last_update


def read_multiple_records(codes, record_dir):
    '''Read records of multiple companies.'''
    result = list()
    for code in codes:
        data, = read_records(code, record_dir)
        result.append((code, data))
    return result


def read_all_records(code_file, record_dir):
    '''Read records of all codes.

    Returns a list of (code, data) pairs.
    '''
    if not os.path.exists(code_file):
        raise Exception('Code file not exists.')
    if (not os.path.exists(record_dir)) or (not os.path.isdir(record_dir)):
        raise Exception('Record directory not exists.')

    code_n_data = list()
    codes, _ = read_codes(code_file)
    for code in codes:
        data, _ = read_records(code, record_dir)
        if data is not None:
            code_n_data.append((code, data))
    return code_n_data


def update_records(code, record_dir, verbose = True):
    '''Appends records of a company.

    Appends stock price records of given company code.
    '''
    record_file = record_dir + '/' + code + '.txt'
    data, last_update = read_records(code, record_dir)
    last_date = date_end
    try:
        if verbose:
            print 'Updating %s from %s to %s ...' % \
                (code, str(last_update), str(last_date))
        if last_date < last_update:
            raise Exception('Last update data is behind end date.')
        # Appends new data to record file.
        new_data = pd.DataReader(
            code + '.KS', 'yahoo', last_update, last_date)
        new_data = new_data.dropna(axis = 'index', how = 'any')
        with open(record_file, 'a') as f:
            new_data.to_csv(f, header = (data.size == 0))
    except Exception as e:
        error_codes.append(code)
        print 'Error in code ' + code + ': ' + str(e)


def update_codes(code_file, record_dir):
    '''Updates records.

    Updates price records for all codes in the [code_file].
    Result files are stored in [record_dir].
    '''
    if not os.path.exists(code_file):
        raise Exception('Code file not exists.')
    if (not os.path.exists(record_dir)) or (not os.path.isdir(record_dir)):
        raise Exception('Record directory not exists.')

    codes, _ = read_codes(code_file)
    for code in codes:
        update_records(code, record_dir)
    print error_codes


#
# Analysis
#


def change_rate(values, target, slide, length):
    '''Returns last [length] number of change rates of [slide] days window
    for the [target] code.
    '''
    return values[target].pct_change(slide).dropna().tail(length) * 100


def analyze_target(records, **conf):
    '''Analyzes a target code.'''
    # rates = DataFrame()
    rates = change_rate(records, conf['col'], conf['slide'], conf['days'])
    print rates.nsmallest(5)
    print rates.nlargest(5)
    # ups = rates.select(lambda x: x > 0.0)
    # downs = rates.select(lambda x: x < 0.0)
    # print ups.size, downs.size
    # print rates.gt(0.0).value_counts()


def analyze_all(records, **conf):
    '''Analyze all codes.

    records: a list of (code, dataframe) pairs.
    '''
    rates = DataFrame()
    for code, values in records:
        try:
            rates[code] = change_rate(
                values, conf['col'], conf['slide'], conf['days'])
        except Exception as e:
            print 'Error in %s: %s' % (code, str(e))
            raise e

    # Up-Down
    print '=== Up-downs ==='
    counts = DataFrame()
    for code, rate in rates.iteritems():
        counts[code] = rate.gt(0).value_counts()
    print 'Most ups:'
    print counts.transpose()[True].nlargest(10)
    print 'Least ups:'
    print counts.transpose()[True].nsmallest(10)
    # print counts.transpose()[False].nlargest(10)

    # Hike
    print '=== Hike ranking ==='
    hike_counts = DataFrame()
    for code, rate in rates.iteritems():
        hike_counts[code] = rate.apply(lambda x: x >= 2.0).value_counts()
    print hike_counts.transpose()[True].nlargest(20)

    # print '==== plot rates ===='
    plot_items(rates, 2, 400)

    # Note: correlation is not worth to study.
    # print '==== highest correlations ===='
    # smoothed = rates.applymap(lambda x: round(x / 5.0))
    # sims = smoothed.corr()
    # sims = sims[sims != 1]
    # print sims.head().stack().nlargest(5)

    # print '==== highest liquidities(variances) ===='
    # var_highest = rates.abs().sum().nlargest(5)
    # print var_highest
    # highest_rates = dict()
    # for code, value in var_highest.iteritems():
    #     highest_rates[code] = rates[code]
    # # plot_items(highest_rates, 5, 200)

    # print '==== lowest liquidities(variances) ===='
    # var_lowest = rates.var().nsmallest(5)
    # print var_lowest
    # lowest_rates = dict()
    # for code, value in var_lowest.iteritems():
    #     lowest_rates[code] = rates[code]
    # # plot_items(lowest_rates, 5, 200)


#
# Plot
#

def plot_items(data, count, series_length):
    handles = list()
    for code, rates in data.iteritems():
        p_h, = plt.plot(rates.tail(series_length), label = code)
        handles += [p_h]
        if --count == 0:
            break
    plt.legend(handles)
    plt.show()


#
# Driver
#

if __name__ == '__main__':
    code_file = 'kospi.txt'
    record_dir = 'kospi_records'

    # Data update
    # date_begin = datetime.date(2014, 1, 1)
    # date_end = datetime.date.today()
    # update_codes(code_file, record_dir)

    # Full data analysis
    all_records = read_all_records(code_file, record_dir)
    analyze_all(all_records, col = 'Close', slide = 5, days = 200)

    # Target analysis
    # target_codes = ['002240']
    # data, _ = read_records('002240', record_dir)
    # analyze_target(data, col = 'Close', slide = 1, days = 900)
