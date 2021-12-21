# Z0096


# import standard libraries
import pandas as pd
import numpy as np

# import file checker
from os.path import isfile

# import the pickles
import pickle

# import sql connector
from env import get_connection


#################### Wrangle Data ####################


def make_pickles(py_object, filename):
    '''

    Takes in argument for python object and associated filename as 
    string and pickles object with "filename.pickle"

    '''

    # dump obtained DataFrame into pickle file
    pickle_out = open(f'{filename}.pickle', 'wb')
    pickle.dump(py_object, pickle_out)
    pickle_out.close()


def open_pickles(filename):
    '''

    Takes in filename as string of previously pickled object and
    returns opened pickle jar as a python object

    '''

    # load existing DataFrame from pickle file
    pickle_in = open(f'{filename}.pickle', 'rb')
    opened_jar = pickle.load(pickle_in)
    pickle_in.close()

    return opened_jar


def wrangle_curriculum(use_pickles=True):
    '''
    '''

    # check if pickled data already exists
    if isfile('curriculum_logs.pickle') == False or use_pickles == False:
        # settting column names for reading in .txt file using read_csv function
        cols = ['date', 'endpoint', 'user_id', 'cohort_id', 'source_ip']
        # set arguments to seperate on spaces, pass column names from list, use cols at set index location
        df = pd.read_csv('anonymized-curriculum-access-07-2021.txt', 
                         sep='\s', 
                         header=None, 
                         names = cols, 
                         usecols=[0, 2, 3, 4, 5])
        # set query to get cohorts table
        query = 'SELECT * FROM cohorts;'
        # set url for sql db
        url = get_connection('curriculum_logs')
        # read in cohorts data as DataFrame
        cohort_df = pd.read_sql(query, url)
        # merge DataFrame with cohort data and Dataframe with user data
        df = pd.merge(left=df,
                      right=cohort_df,
                      how='left',
                      left_on='cohort_id',
                      right_on='id')\
               .drop(columns=['id',
                              'slack',
                              'created_at',
                              'updated_at',
                              'deleted_at'])
        # rename column for cohort name
        df = df.rename(columns={'name':'cohort_name'})
        # convert column with dates into datetime data type
        df.date = pd.to_datetime(df.date)
        df.start_date = pd.to_datetime(df.start_date)
        df.end_date = pd.to_datetime(df.end_date)
        # set date column as index
        df = df.set_index(df.date).sort_index()
        df = df.drop(columns='date')
        # pickle dataframe
        make_pickles(df, 'curriculum_logs')
    # if pickled data is used
    else:
        # load existing DataFrame from pickle file
        df = open_pickles('curriculum_logs')

    return df
