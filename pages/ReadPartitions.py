from fileinput import filename
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

# baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
baseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'

def main():
    with st.form("my_form"):
        fileName = st.text_input('Enter the File Name')
        partitionNumber = st.text_input('Enter the Partition Number')
        submitted = st.form_submit_button("Submit")

    if submitted:
       st.write('Searching for the partition'+partitionNumber+'of', fileName)
       
       #search the Parent direction in the which the file located. 
       previousNode = json.loads(requests.get(baseURL+"/Metadata/"+str(fileName)+"1/previousNode.json").text)
     
       path = "";
       
       while previousNode != 0:
            #get the parent directory
            previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo=1').text)
            #update the previousNode
            directoryName = list(previousNodeData.keys())[0]
            previousNode = previousNodeData[directoryName]['previousNode']
            path = directoryName+"/"
            
       # get the data from the partition
       finalPath = path+fileName+str(partitionNumber) 
       data = json.loads(requests.get(baseURL+"/Data/"+directoryName+"/"+fileName+str(partitionNumber)+".json").text)
       json_object = json.dumps(data) 
       #print(json_object)
       df = pd.read_json(json_object, orient ='records')
       st.write(df)
       #print(data)
       
       
       

if __name__=="__main__":
    main()
