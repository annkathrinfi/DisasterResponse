# Disaster Response
Second project for Udacity Data Science Nanodegree. A ETL pipeline, NLP and Machine Learning pipeline were built to classify disaster related messages data into different categories.

## **Table of Contents:**
1. [Project Overview](README.md#project-Overview)
2. [File Description](README.md#file-description)
3. [Instructions](README.md#Instructions)
4. [Libraries used](README.md#libraries-used)
5. [Results](README.md#results)
6. [Licensing, Acknowledgements](README.md#licensing-acknowledgements)

## **Project Overview**<br/>

In this project, data provided by [Appen](https://appen.com/) are used to build a model for an API that classifies disaster messages. <br/>
The data contains pre-labeled text messages that were sent during disaster events. <br/>
Objective is to prepare the data with ETL(Extract, Transform, Load) pipeline & then use a ML(Machine Learning) pipeline to build a supervised learning model to categorize the events. This will help emergency workers to send the messages to appropriate disaster relief agency.

## **File Description**<br/>

The following files store the data used in this project:
1) disaster_messages.csv: Messages dataset containing 4 columns including the message id, translated message, original message, and the type of message (e.g. news, social media).<br/>
2) disaster_cateogires.csv: Categories dataset containing 2 columns including the message id and the category(ies) of the message.<br/>
3) DisasterResponse.db: SQLite database where the wrangled data is loaded to by the ETL script and from which the training and test data is loaded from in the ML pipeline script.<br/>

There are three python scripts used to deploy on the workspace:<br/>
1) process_data: Contains functions with executed code from ETL Pipeline<br/>
2) train_classifer: Contains functions with executed code from ML Pipeline<br/>
3) run: Contains model pkl file and code for visualizations to run the web app<br/>

## **Instructions**<br/>

1) To run ETL pipeline that cleans data and stores in database:<br/>
python data/process_data.py data/disaster_messages.csv data/disaster_categories.csv data/DisasterResponse.db<br/>
2) To run ML pipeline that trains classifier and saves<br/>
python models/train_classifier.py data/DisasterResponse.db models/classifier.pkl<br/>
3) To run web app in the app's directory<br/>
python run.py<br/>
4) URL to see visualization<br/>
http://localhost:3000/<br/>


## **Libraries Used**<br/>

Following libraries were used:<br/>
Plotly<br/>
joblib<br/>
Pandas<br/>
Numpy<br/>
nltk<br/>
flask<br/>
sqlalchemy<br/>
sys<br/>
scikit-learn<br/>

## **Results**<br/>
End result is a web app powered by supervised machine learning model which:<br/>
A) Contains Visualizations
B) When a message is entered, classfies into different groups.
Ex: Type:  we are more than 50 people on the street. Please help us find tent and food & Click "Classify Message" Button

![Screenshot](Classifying_text.jpg)
![Screenshot](Viz2.jpg)
![Screenshot](Viz3.jpg)
![Screenshot](Viz1.jpg)

## **Licensing, Acknowledgements**<br/>
Thanks to real life disaster messages data from Appen.<br/>
Thanks to Udacity for providing knowledge on Data Engineering (ETL/NLP/ML Pipelines) and a platform to work on this project.<br/>
