import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
# FUNCTION -- mkdir: create a directory in file system, e.g., mkdir /user/john
# create a directory in both the databases.

# Cheks if directory already present else creats the directory.

def createDirectory(directories):
    #Iterating from 1st position and checking if the directory already exixts.
    # if not, the creating the directory in the Data and MetaData sections
    path = ""
    prevID = 0
    for directory in directories:
        list = json.loads(requests.get(baseURL+"/Metadata/"+directory+".json").text)
        if(list == None):
            #MetaData
            # getting the ID of the LastNode.
            lastNode_ID = int(json.loads(requests.get(baseURL+"/Metadata/Lastnode.json").text))
            res1 = requests.put(baseURL+"/Metadata/"+directory+"/.json", '{"id":'+str(lastNode_ID+1)+', "type": "DIRECTORY","previousNode":'+str(prevID)+'}')
            prevID = lastNode_ID+1
            #update the last node
            res2 = requests.patch(baseURL+"/Metadata.json", '{"Lastnode":'+str(lastNode_ID+1)+'}')
            #creating the directory in data section
            res3 = requests.patch(baseURL+"/Data"+path+".json",'{"'+str(directory)+'":""}')
            print(baseURL+"/Data"+path+".json")
            path += "/"+directory
        else:
            prevID = list["id"]
            path += "/"+directory


def createDirectoryMonoDB(directories):

    #Creating DB Connection 
    client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
    db = client.proj #establish connection to the  db

    pathDict = {}
    pathDictFull = pathDict
    prevID = 0
    for directory in directories:
        dataFetched = list(db.Metadata.find({ directory: {"$exists": True}}))
        if len(dataFetched):
            # DIRECTORY EXISTS
            # get the Id Value of the Node and update the prevID
            prevIDDict = list(db.Metadata.find({},{"_id":0,str(directory)+".id":1}))
            for d in prevIDDict:
                if len(d) != 0:
                    prevID = d[str(directory)]["id"]
        
            #Updating pathDict 
            pathDict[str(directory)] = {}
            pathDict = pathDict[str(directory)] 

        else:
            #DIRECTORY DOESN'T EXISTS
            #Getting the value of the lastnode
            LastnodeDict = db.Metadata.find_one({},{'_id':0,'Lastnode':1})

            #Adding meta data about the directory in Metadata section
            dictMetadata = {directory :{"id": str(int(LastnodeDict["Lastnode"])+1) , "type":"DIRECTORY","previousNode":str(prevID)}}
            resM = db.Metadata.insert_one(dictMetadata)
            
            #Creating the path dict for the data section
            pathDict[str(directory)] = {}
            pathDict = pathDict[str(directory)] 

            #updating PrevID so we can trace the path 
            prevID = int(LastnodeDict["Lastnode"])+1

            #Updating the value of the Lastnode
            oldval = { "Lastnode": LastnodeDict["Lastnode"]}
            newval = { "$set": { "Lastnode": str(int(LastnodeDict["Lastnode"])+1)}}
            db.Metadata.update_one(oldval, newval)

        #Updating the data node 
    resD = db.Data.insert_one(pathDictFull)
    

def main():
    
    with st.form("my_form"):
        filePath = st.text_input('Enter a path to create the directory:', "eg: User/john")
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('Creating directory...')
        directories = filePath.split("/") # getting the names of all the directory
        ### CREATING DIRECTORY IN FIREBASE ###
        createDirectory(directories) 
        ### CREATING DIRECTORY IN MONGODB ###
        createDirectoryMonoDB(directories)




if __name__=="__main__":
     main()