import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://dsci551-b0e35-default-rtdb.firebaseio.com/'
mongoURL = 'mongodb+srv://yinhanli:1234@dsci551.avoigkh.mongodb.net/?retryWrites=true&w=majority'

#function to convert string with other symbols into float
def convert(mystr):
    first = mystr.replace(',', '')
    second = first.replace('%', '')
    return float(second)
#map partition method
def mapPartition(url, dataset, partition, country, feature):
    datalist = []
    intPartition = int(partition)
    loc = ""
    if dataset == "universities_ranking_P":
        loc = "location"
    if dataset == "CountryDataP":
        loc = "Region"
    if dataset == "GDPExpendenditure_in_percentage_R":
        loc = "Year"
    if country == "Total":
        for i in range(intPartition):
            n = str(i + 1)
            datas = json.loads(requests.get(baseURL + "Data" + "/" + "User" + "/" + dataset + n + "/.json").text)
            count = 0
            for data in datas:
                count += 1
            datalist.append(count)
        return datalist
    for i in range(intPartition):
        n = str(i + 1)
        datas = json.loads(requests.get(baseURL + "Data" + "/" + "User" + "/" + dataset + n + "/.json").text)
        count = 0
        for data in datas:
            if data[loc] == country:
                count += 1
        datalist.append(count)
    return datalist

def reduceCount(datalist):
    result = 0
    for l in datalist:
        result += l
    return result

def main():
    with st.form("my_form"):
        dbURL = st.selectbox('Which data base are you querying from:', ('firebase', 'mongodb'))
        datasetName = st.selectbox('Which data set do you want to analyze:', ('universities_ranking_P', 'CountryDataP', 'GDPExpendenditure_in_percentage_R'))
        parti = st.text_input('How many partitions in this dataset:', '')
        countryName = st.text_input('Which Country or Region:', 'eg: United States')
        featureName = st.text_input('Which feature do you want to count:', 'eg: number students')

        submitted = st.form_submit_button("Submit")

        if submitted:
            st.write('Computing...')
            intermediateList = mapPartition(baseURL, datasetName, parti, countryName, featureName)
            result = reduceCount(intermediateList)
            st.write('The Count is: ', result)
            st.write('The intermediate output of mapPartition is a list of list with length of the number of partition:', intermediateList)
            st.write('Then the reduce function count the final number:', result)





















if __name__=="__main__":
    main()
