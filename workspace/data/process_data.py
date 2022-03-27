import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    '''
    Description: Loads two csv files and then merges them into one dataframe using the 'inner'-method.
    Input: 2 csv files
    Output: 1 dataframe
    '''
    messages = pd.read_csv(messages_filepath,sep=",")
    messages.drop_duplicates(subset=['id'],inplace=True)
    categories = pd.read_csv(categories_filepath)
    categories.drop_duplicates(subset=['id'],inplace=True)
    df = pd.DataFrame()
    if not 'message' in df.columns:
        df = pd.merge(categories, messages, on = 'id', how = 'inner')
        print('Merge completed')
        df.head()
    return df

def clean_data(df):
    '''
    Split the values in the categories column on the character ';' so that each value becomes a separate column
    Use the first row of categories dataframe to create column names for the categories data
    '''
    #global categories
    # create a dataframe of the 36 individual category columns
    categories  = df['categories'].str.split(';', expand=True)
    # select the first row of the categories dataframe
    row = categories.iloc[0]
    # use this row to extract a list of new column names for categories.
    category_colnames = row.apply(lambda x : x[:-2])
    # rename the columns of `categories`
    categories.columns = category_colnames
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]
        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])
    # drop the original categories column from `df`
    df = df.drop('categories', axis=1)
    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df,categories],axis=1)
    # replace entry 2 with 1 in 'related' column
    df['related'].replace(2,1,inplace=True)
    print('Number of duplicates: {}'.format(np.sum(df.duplicated(subset=['id']))))
    return df


def save_data(df, database_filename='DisasterResponse.db', table_name = 'DisasterResponseData'):
    '''
    Description: Stores the data in a table in a specified SQLite database.
    Input: Dataframe contained wrangled data 
         : Name of database    
    Ouput: Database with data table
    '''   
    #InsertTableName = database_filename.split('.')[0]
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return df


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()