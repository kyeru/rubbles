# -*- coding: utf8 -*-

import datetime
import math
import os
import pandas
import pandas_datareader.data as pd
from pandas import DataFrame, Timestamp
import matplotlib.pyplot as plt

import kospi_code


#
# Global variables
#

date_begin = datetime.date(2014, 1, 1)
date_end = datetime.date.today()


#
# Data Setup
#

def read_records(code, record_file):
    '''Reads records of a company and return as a DataFrame object.'''
    if not os.path.exists(record_file):
        f = open(record_file, 'w')
        f.close()
        
    with open(record_file, 'r') as f:
        last_update = date_begin
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


def get_data_list(codes, record_dir):
    '''Read records of given multiple companies.

    Returns a list of (code, dataframe) pairs.'''
    if (not os.path.exists(record_dir)) or (not os.path.isdir(record_dir)):
        raise Exception('Record directory not exists.')

    data_list = list()
    for code, name in codes:
        record_file = record_dir + '/' + code + '.txt'
        data, _ = read_records(code, record_file)
        data_list.append((code, data))
    return data_list


def update_records(code, record_file, verbose = True):
    '''Appends records of a company.

    Appends stock price records of given company code.
    Returns True if succeeded, False otherwise.
    '''
    data, last_update = read_records(code, record_file)
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
        print 'Error in code ' + code + ': ' + str(e)
        return False
    return True


def update_record_files(codes, record_dir):
    '''Updates records.

    Updates stock price records for given codes.
    Result files are stored in [record_dir].
    '''
    if (not os.path.exists(record_dir)) or (not os.path.isdir(record_dir)):
        raise Exception('Record directory not exists.')

    error_codes = list()
    for code, name in codes:
        record_file = record_dir + '/' + code + '.txt'
        if not update_records(code, record_file):
            error_codes.append(code)
    print 'Error in\n' + str(error_codes)


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
            rates[code] = change_rate(values, 'Close', 1, 500)
        except Exception as e:
            print 'Error in %s: %s' % (code, str(e))
            raise e

    # test
    # for code, values in records:
    #     mean = values['Close'].tail(300).aggregate('mean')
    #     last = values['Close'].tail(1).aggregate('min')
    #     if code in kospi_code.kospi200map and last < mean * (1 - 0.1):
    #         print '<div><a href="http://finance.naver.com/item/main.nhn?code=' \
    #             + code + '">' + kospi200map[code] + '</a></div>'

    # Up-Down
    # print '=== Up-downs ==='
    # counts = DataFrame()
    # for code, rate in rates.iteritems():
    #     counts[code] = rate.gt(0).value_counts()
    # print 'Most ups:'
    # print counts.transpose()[True].nlargest(10)
    # print 'Least ups:'
    # print counts.transpose()[True].nsmallest(10)
    # print counts.transpose()[False].nlargest(10)

    # Hike
    print '=== Hike ranking ==='
    hike_counts = DataFrame()
    h1 = DataFrame()
    h2 = DataFrame()
    # h3 = DataFrame()
    # h4 = DataFrame()
    # h5 = DataFrame()
    for code, rate in rates.iteritems():
        h1[code] = rate.tail(30).apply(lambda x: x > 0.5 and x < 1.5).value_counts()
        h2[code] = rate.tail(60).apply(lambda x: x > 0.5 and x < 1.5).value_counts()
        # h3[code] = rate.tail(300).apply(lambda x: x > 0.5).value_counts()
        # h4[code] = rate.tail(400).apply(lambda x: x > 1.0).value_counts()
        # h5[code] = rate.tail(500).apply(lambda x: x > 1.0).value_counts()
    h1 = h1.transpose()[True].nlargest(30)
    h2 = h2.transpose()[True].nsmallest(h2.size - 30)

    # h3 = h3.transpose()[True]
    # h3 = h3.nsmallest(h3.size - 50)
    # h4 = h4.transpose()[True].nlargest(50)
    # h5 = h5.transpose()[True].nlargest(50)
    
    # h1, _ = h1.align(h2, axis = 0, join = 'inner')
    h1, _ = h1.align(h2, axis = 0, join = 'inner')
    # h1, _ = h1.align(h4, axis = 0, join = 'inner')
    # h1, _ = h1.align(h5, axis = 0, join = 'inner')
    for code, value in h1.iteritems():
        print code, value
    
    # print '==== plot rates ===='
    # plot_items(rates, 2, 400)

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


#
# Plot
#

def plot_items(data, count, series_length):
    handles = list()
    number = 1
    for code, rates in data.iteritems():
        if number > count:
            break
        p_h, = plt.plot(rates.tail(series_length), label = code)
        handles += [p_h]
        number += 1
    plt.legend(handles)
    plt.show()


#
# Driver
#

if __name__ == '__main__':
    record_dir = 'kospi_records'

    # Data update
    # date_begin = datetime.date(2014, 1, 1)
    # date_end = datetime.date.today()
    # update_record_files(kospi_code.kospi200, record_dir)

    # Full data analysis
    data_list = get_data_list(kospi_code.kospi200, record_dir)
    analyze_all(data_list, col = 'Close', slide = 1, days = 200)

    # Target analysis
    # target_codes = ['002240']
    # data, _ = read_records('002240', record_dir)
    # analyze_target(data, col = 'Close', slide = 1, days = 900)
