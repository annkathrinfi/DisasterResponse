# import libraries
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import re
import pickle

import nltk
nltk.download('stopwords')
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from sklearn.pipeline import Pipeline
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import GridSearchCV

def load_data(database_filepath):
    '''
    Description: Loads data from the SQLite database
    Input: Database name
    Output: Features(X), labels(y) and cateogry_names
    '''
    engine = create_engine('sqlite:///data/DisasterResponse.db')
    df = pd.read_sql_table('DisasterResponseData', engine)
    X = df['message']
    y = df.iloc[:,4:]
    category_names = y.columns.tolist()
    return X, y, category_names


def tokenize(text):
    '''
    Description: Normalise, lemmatize and tokenize text from messages.
    Input: Text data
    Output: Normalised, lemmatized and tokenized text
    '''
    stop_words = stopwords.words("english")
    lemmatizer = WordNetLemmatizer()
    # normalize case and remove punctuation
    text = re.sub(r"[^a-zA-Z0-9]", " ", text.lower())
    # tokenize text
    tokens = word_tokenize(text)
    # lemmatize andremove stop words
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word not in stop_words]
    return tokens


def build_model():
    '''
    Description: Create text processing and machine learning pipeline that uses the custom tokenize function in the ML pipeline to vectorize
    and transform text. MultiOutputClassifier to support multi-target classification using LinearSVC classifier to enables predictions on 36
    categories. Use GridSearchCV to select the best parameters for the classifier.
        
    Output: Text processing and ML pipeline 
    '''
    
    forest = RandomForestClassifier(n_estimators=10, random_state=1)
    
    pipeline = Pipeline([
        ('vect', CountVectorizer(tokenizer=tokenize)),
        ('tfidf', TfidfTransformer()),
        ('clf', MultiOutputClassifier(forest))
        ])
    
    parameters = {'clf__estimator__n_estimators': [5, 10]}


    cv = GridSearchCV(pipeline, param_grid=parameters)
    
    return cv


def evaluate_model(model, X_test, Y_test, category_names):
    '''
    Description: Use ML pipeline to predict labels of test features and produce classification report containing precision, recall, f1 score for each category.
    Report best parameters tested using GridSearchCV.
    
    Input: ML pipeline, test and label features and category_names
    
    Output: F1 score, precision and recall for each category in test set 
    
    '''
    Y_pred = model.predict(X_test)
    for i in range(36):
        cr = classification_report(Y_test.iloc[:, i], Y_pred[:, i],target_names=category_names)
        print(cr)


def save_model(model, model_filepath):
    '''
    Description: Exports the final model as a pickle file
    Input: ML pipeline, name of pickle file
    Output: Pickle file
    '''    
    filename = model_filepath
    
    return pickle.dump(model, open(filename, 'wb'))


def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, Y, category_names = load_data(database_filepath)
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)
        
        print('Building model...')
        model = build_model()
        
        print('Training model...')
        model.fit(X_train, Y_train)
        
        print('Evaluating model...')
        evaluate_model(model, X_test, Y_test, category_names)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__':
    main()