import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'

def main():
    # Getting all the files present the database for querying
    FileNames = json.loads(requests.get(baseURL+"/FileLocations.json").text) 
    CurrentFiles = list(FileNames.keys())
    CurrentFiles.remove("Count")
    
    buttons = []
    if "Selected" not in st.session_state:
        st.session_state.Selected = -1

    for i in range(len(CurrentFiles)):
        buttons.append(st.button(str(CurrentFiles[i])))
        st.session_state.Selected = i


    for i, button in enumerate(buttons):
        if button:
            st.session_state.Selected = i
            st.write("Loading contents of "+(CurrentFiles[i])+" from the File System.....")

            #Checking the Database in which the file is present.
            dbName = json.loads(requests.get(baseURL+"FileLocations/"+CurrentFiles[i]+"/dbname.json").text)
            st.write("***"+(CurrentFiles[i])+"*** is located on ***"+dbName+"***")

            #Extracting the nume of partitions of the file..
            numOfPartitions = json.loads(requests.get(baseURL+"FileLocations/"+CurrentFiles[i]+"/partitions.json").text)
            st.write(" The file is stored as ***"+str(numOfPartitions)+"*** partitions")

            #Determining the path at which the file is stored.
            if dbName == "FIREBASE":
                #search the Parent directory in the which the file located.
                previousNode = json.loads(requests.get(baseURL+"/Metadata/"+str(CurrentFiles[i])+"1/previousNode.json").text)
                path = ""

                #while previousNode != 0:
                #get the parent directory
                previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo='+str(previousNode)).text)
                #update the previousNode
                directoryName = list(previousNodeData.keys())[0]
                previousNode = previousNodeData[directoryName]['previousNode']
                path = directoryName+"/"

                # Location of Partitions:
                #for i in range(numOfPartitions):
                #    st.write(path)


       


    #with st.form("my_form"):
    #    st.write("Form")

if __name__=="__main__":
     main()