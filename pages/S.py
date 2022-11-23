import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'

def countCountry(dataframe):
        df1 = dataframe[dataframe["ranking"] < 100].groupby('location')['location'].count()
        #df1 = dataframe[dataframe["ranking"] < 30]
        st.write(df1)

def main():
    
    with st.form("my_form"):
        filePath = st.text_input("Enter the FileName")
        submitted = st.form_submit_button("Submit")

    if submitted:
        #check in which database the file is present
        fileName = "universities_ranking_P"
        dbName = json.loads(requests.get(baseURL+"FileLocations/"+"universities_ranking_P"+"/dbname.json").text)
        numOfPartitions = 2

        if(dbName == "FIREBASE"):
            #search the Parent directory in the which the file located. 
            previousNode = json.loads(requests.get(baseURL+"/Metadata/"+str(fileName)+"1/previousNode.json").text)
            path = ""
            while previousNode != 0:
                #get the parent directory
                previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo=1').text)
                #update the previousNode
                directoryName = list(previousNodeData.keys())[0]
                previousNode = previousNodeData[directoryName]['previousNode']
                path = directoryName+"/"
            
            # get the data from the partition
            for i in range(1,numOfPartitions+1):
                data = json.loads(requests.get(baseURL+"/Data/"+directoryName+"/"+fileName+str(i)+".json").text)
                json_object = json.dumps(data) 
                df = pd.read_json(json_object, orient ='records')
                countCountry(df)
                #st.write(df)

    #Select Universisties having Ranking between 20 and 30.
    #Number of universities in each country having rank betwwen 20 and 30.
   
        
        

    #result = list(map(countCountry,data))
    #filter univeristy names and Ranking:

       
if __name__=="__main__":
     main()
