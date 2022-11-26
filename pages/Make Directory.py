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
    # if not, the creating the directory.
    path = ""
    prevID = 0
    for directory in directories:
        list = json.loads(requests.get(baseURL+"/Metadata/"+directory+".json").text)
        if(list == None):
            #MetaData
            # getting the ID of the LastNode.
        
            #print(prevID,"INITIAL PREVID INSIDE IF")
            lastNode_ID = int(json.loads(requests.get(baseURL+"/Metadata/Lastnode.json").text))
            res1 = requests.put(baseURL+"/Metadata/"+directory+"/.json", '{"id":'+str(lastNode_ID+1)+', "type": "DIRECTORY","previousNode":'+str(prevID)+'}')
            #updating PrevID
            prevID = lastNode_ID+1
            #print(prevID,"FINAL PREV ID INSIDE ELSE")
            #update the last node
            res2 = requests.patch(baseURL+"/Metadata.json", '{"Lastnode":'+str(lastNode_ID+1)+'}')
            #creating the directory in data section
            res3 = requests.patch(baseURL+"/Data"+path+".json",'{"'+str(directory)+'":""}')
            #print(baseURL2+"/Data"+path+".json")
            path += "/"+directory
            #print(res1,res2,res3,"RES1&2&3")
        else:
            prevID = list["id"]
            path = "/"+directory


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
        
            #getting the Contents of this Directory.
            currentDirectoryContents = list(db.Data.find({},{str(directory):1}))
            currDict = currentDirectoryContents[0]
            try:
                pathDict[str(directory)] = currDict[str(directory)]
            except:
                pathDict[str(directory)] = {}
            #Updating pathDict 
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
    dataFetched = list(db.Data.find({directories[0]:{"$exists": True }}))
    if len(dataFetched):
        res = db.Data.delete_one(dataFetched[0])
    resD = db.Data.insert_one(pathDictFull)


def LoadData():
    #st.write("Existing Directories...")
    #extracting all the Directories in Firebase:
    st.write("Loading File System Structure...")
    fireBaseDirectories =json.loads(requests.get(baseURL+"Metadata/.json").text)
    Keys = fireBaseDirectories.keys()
    FB_DirectoryList = []
    for i in Keys:
        #Building Directory of each Nodes.
        if i != "Lastnode" and fireBaseDirectories[i]["type"] != "FILE":
            path = ""
            previousNode = fireBaseDirectories[i]["previousNode"]
            if previousNode == 0:
                path = i+"/"
                FB_DirectoryList.append(path)
            else:    
                while previousNode != 0:
                    #get the parent directory
                    previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo='+str(previousNode)).text)
                    #update the previousNode
                    directoryName = list(previousNodeData.keys())[0]
                    previousNode = previousNodeData[directoryName]['previousNode']
                    path = directoryName+"/"

                path = path + i+"/"
                FB_DirectoryList.append(path)
                
    
    #st.write(FB_DirectoryList,"DIRECTORY LIST")
    
    #extracting all the Directories in MongoDB:
    # Connecting to MongodB
    client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
    db = client.proj #establish connection to the  db

    #getting all the entries in MetdaData Section
    Metadata = db["Metadata"]
    MetadataObj = list(Metadata.find({},{"_id":0}))
    # Storing all the keys from the MetaData into KeyList
    #print(cursor)
    #st.write(MetadataObj,"DATAObj")
    KeyList = []
    for i in MetadataObj[1:]:
        tempList = list(i.keys())
        if i[tempList[0]]["type"] == "DIRECTORY":
            KeyList.append(tempList[0]) 
    
    MDB_Directories = []
        
    #Building Each Path 
    for name in KeyList:
        previousNode = list(db.Metadata.find({},{"_id":0,name+".previousNode":1}))
        prevdirectoryId = ""
        for j in previousNode:
           if len(j) != 0:
                prevdirectoryId = int(j[name]['previousNode'])
            
    #building path to file from root
        if prevdirectoryId == 0:
            MDB_Directories.append(name+"/")
        else:
            path = ""
            while prevdirectoryId != 0:
                for k in KeyList:
                    pipeline = [{"$match": {k+".id":{"$eq":str(prevdirectoryId)}}},{"$project":{"_id":0}}]
                    result = list(Metadata.aggregate(pipeline))
                    if len(result) != 0:
                        prevdirectoryId = int(result[0][k]["previousNode"])
                        path = path+k+"/"
                        break
            MDB_Directories.append(path+name+"/")

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Existing directories on Firebase**")
        for i in FB_DirectoryList:
            st.write(i)
        st.write("-End-")
    with col2:
        st.write("**Existing directories on MongoDB**")
        for i in MDB_Directories:
            st.write(i)
        st.write("-End-")

    Firebase_directoryNames_Single = []
    for i in FB_DirectoryList:
        Firebase_directoryNames_Single.extend(i.split("/"))
    #Removing Empty Strings
    Firebase_directoryNames_Single = ' '.join(Firebase_directoryNames_Single).split()
    
    MongoDb_directoryNames_Single = []
    for i in MDB_Directories:
        MongoDb_directoryNames_Single.extend(i.split("/"))
    #Removing Empty Strings
    MongoDb_directoryNames_Single = ' '.join(MongoDb_directoryNames_Single).split()
    return(FB_DirectoryList,MDB_Directories,Firebase_directoryNames_Single,MongoDb_directoryNames_Single)

def main():
    with st.form("my_form"):
        filePath = st.text_input('Enter a path to create directory:')
        st.write("eg: User/John/")
        submitted = st.form_submit_button("Submit")
    
    placeholder = st.empty()
    with placeholder.container():
        FB_DirectoryList,MDB_Directories,FB_Single, MDB_Single = LoadData()


    if submitted:
        if filePath == None:
            st.error("Error in input - Reload the page and Try Again")
        else:
            directories = filePath.split("/") # getting the names of all the directory
            directories = ' '.join(directories).split()
            if("Data" in directories or "Metadata" in directories or "Lastnode" in directories or "FileLocations" in directories):
                st.write('Directory Added')
                st.write("Cannot create this directory. Create a directory with different names")
            elif (filePath in FB_DirectoryList) and (filePath in MDB_Directories):
                st.write('Directory **NOT** Added')
                st.error("***This directory already exists in both the databases.***")
            elif (filePath in FB_DirectoryList):
                #create directory only in Mongodb
                if (set(directories) <= set(MDB_Single)):
                    st.error("Cannot create the directory on MongoDB. Directory Name already Exists")
                else:
                    st.write('Directory Added')
                    createDirectoryMonoDB(directories)
                    st.write("***The directory has been created on MongoDB***")
            elif (filePath in MDB_Directories):
                ### CREATING DIRECTORY IN FIREBASE ###
                if (set(directories) <= set(FB_Single)):
                    st.error("Cannot create the directory on Firebase. Directory Name already Exists")
                else:
                    st.write('Directory Added')
                    createDirectory(directories) 
                    st.write("***The directory has been created on Firebase***")
            else:
                ### CREATING DIRECTORY IN FIREBASE ###
                createDirectory(directories) 
                ### CREATING DIRECTORY IN MONGODB ###
                #st.write('Directory Added')
                createDirectoryMonoDB(directories)
                st.write('Directory Added')
                st.write('Created directory in both Databases')
        
        with placeholder.container():
            FB_DirectoryList,MDB_Directories, _, _ = LoadData()
        #st.write(KeyList,"PRINT")  

    #st.write(Keys)
    #st.write(fireBaseDirectories)
    


if __name__=="__main__":
     main()