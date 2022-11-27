from fileinput import filename
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
#Read all the available Files
def getFilesFromEDFS():
    AllFiles =json.loads(requests.get(baseURL+"FileLocations/.json").text)
    Keys = AllFiles.keys()
    FB_fileNames = []
    for i in Keys:
        #Building Directory of each Nodes.
        if i != "Count":
            partDem = AllFiles[i]["partitions"]
            for num in range(1,int(partDem)+1):
                FB_fileNames.append(i+".csv -  Partition - "+str(num))
    return FB_fileNames


def main():
    with st.form("my_form"):
        FileNamesList = getFilesFromEDFS()
        #Select Box to select the file name
        fileOption = st.selectbox(
           'Select a file name',FileNamesList)

        submitted = st.form_submit_button("Submit")
        placeholder = st.empty()

    if submitted:
        fileName = fileOption.split(".",1)[0]
        partition = fileOption[-1]

        st.write('Searching the locaton of : '+fileName+".csv")
        # getting the num of paritions and the database location of the given file name
        numofPartitions = json.loads(requests.get(baseURL+"/FileLocations/"+fileName+"/partitions.json").text)
        dbLocation = json.loads(requests.get(baseURL+"/FileLocations/"+fileName+"/dbname.json").text)

        if dbLocation == "FIREBASE":
            #search the Parent directory in the which the file located. 
            previousNode = json.loads(requests.get(baseURL+"/Metadata/"+str(fileName)+"1/previousNode.json").text)
            path = ""
       
            while previousNode != 0:
                #get the parent directory
                previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo='+str(previousNode)).text)
                #update the previousNode
                directoryName = list(previousNodeData.keys())[0]
                previousNode = previousNodeData[directoryName]['previousNode']
                path = directoryName+"/"
            
            st.write("The Partition "+ partition+" is located at "+path+" on Firebase")
        else:
            #Connecting to MongoDB
            client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
            db = client.proj #establish connection to the  db
            #getting all the entries in MetdaData Section
            Metadata = db["Metadata"]
            MetadataObj = list(Metadata.find({},{"_id":0}))
            cursor = list(MetadataObj)
            # Storing all the keys from the MetaData into KeyList
            #print(cursor)
            KeyList = []
            for i in cursor[1:]:
                tempList = list(i.keys())
                KeyList.append(tempList[0])   

            #search the Parent directory in the which the file located. 
            previousNode = list(db.Metadata.find({},{"_id":0,fileName+str(1)+".previousNode":1}))
            global prevdirectoryId
            for i in previousNode:
                if len(i) != 0:
                    prevdirectoryId = i[str(fileName)+"1"]['previousNode']
            
            path = []

            while prevdirectoryId != 0:
                for k in KeyList:
                    pipeline = [{"$match": {k+".id":{"$eq":prevdirectoryId}}},{"$project":{"_id":0}}]
                    result = list(Metadata.aggregate(pipeline))
                    if len(result) != 0:
                        prevdirectoryId = int(result[0][k]["previousNode"])
                        path.append(k)         
            
            pathStr = '/'.join(path)
            #st.write("LOCATIONS"); 
            #for i in range(1,numofPartitions+1):
            #    st.write("Partition "+str(i)+":"+pathStr+"/")

            st.write("The Partition "+ partition+" is located at "+pathStr+" on MongoDB")
           


if __name__=="__main__":
    main()