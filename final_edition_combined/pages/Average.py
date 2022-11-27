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
        loc = "Country"
    if country == "Total":
        for i in range(intPartition):
            n = str(i + 1)
            datas = json.loads(requests.get(baseURL + "Data" + "/" + "User" + "/" + dataset + n + "/.json").text)
            valueList = []
            for data in datas:
                value = 0
                if feature in data.keys():
                    value = data[feature]
                valueList.append(value)
            datalist.append(valueList)
        return datalist
    for i in range(intPartition):
        n = str(i + 1)
        datas = json.loads(requests.get(baseURL + "Data" + "/" + "User" + "/" + dataset + n + "/.json").text)
        valueList = []
        for data in datas:
            if data[loc] == country:
                value = 0
                if feature in data.keys():
                    value = data[feature]
                valueList.append(value)
        datalist.append(valueList)
    return datalist

def reduceAVG(datalist):
    result = 0
    for l in datalist:
        individualSum = 0
        length = 0
        for i in l:
            content = i
            if type(i) == str:
                content = convert(content)
            if type(i) == int:
                content = float(content)
            individualSum += content
        result += individualSum
        length += len(l)
    return result/length

def main():
    with st.form("my_form"):
        dbURL = st.selectbox('Which data base are you querying from:', ('firebase', 'mongodb'))
        datasetName = st.selectbox('Which data set do you want to analyze:', ('universities_ranking_P', 'CountryDataP', 'GDPExpendenditure_in_percentage_R'))
        parti = st.text_input('How many partitions in this dataset:', '')
        countryName = st.text_input('Which Country or Region:', 'eg: United States')
        featureName = st.text_input('Which feature do you want to average:', 'eg: number students')

        submitted = st.form_submit_button("Submit")

        if submitted:
            st.write('Computing...')
            intermediateList = mapPartition(baseURL, datasetName, parti, countryName, featureName)
            result = reduceAVG(intermediateList)
            st.write('The Average is: ', result)
            st.write('The intermediate output of mapPartition is a list of lists with length of the number of partition:', intermediateList)
            st.write('Then the reduce function add them all up and then take the average to produce final result: ', result)

if __name__=="__main__":
    main()
