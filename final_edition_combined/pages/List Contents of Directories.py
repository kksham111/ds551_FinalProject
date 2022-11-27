import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
# FUNCTION -- ls: listing content of a given directory, e.g., ls /user
# This function will check if the given directory is present in Firebase and MongoDb
# If directory is present, it will combine the results from both the databases and display the result.


def LoadData():
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
                
    #extracting all the Directories in MongoDB:
    # Connecting to MongodB
    client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
    db = client.proj #establish connection to the  db

    #getting all the entries in MetdaData Section
    Metadata = db["Metadata"]
    MetadataObj = list(Metadata.find({},{"_id":0}))
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
    return(FB_DirectoryList,MDB_Directories)


def main():
    with st.form("my_form"):
        filePath = st.text_input('Enter a vlaid directory to list its content:', "eg: User/john")
        submitted = st.form_submit_button("Submit")

    
    placeholder = st.empty()
    with placeholder.container():
        st.write("Loading all the directories in the file system....")
    with placeholder.container():
        FB_DirectoryList,MDB_Directories = LoadData() 

    if submitted:
        placeholder2 = st.empty()
        with placeholder2.container():
            st.write('Retieveing all the contents of directory...')      
        
        with placeholder2.container():
        ############## FireBase ################
            col3, col4 = st.columns(2)
        
            with col3:
                df_FB = pd.DataFrame() # Creating an empty dataframe to store the contents of the Files.
                #Cheking if the directory exists in Firebase.
                directoryContent = json.loads(requests.get(baseURL+"Data/"+filePath+".json").text)
                if directoryContent == None:
                    st.write('No contents in this directory on Firebase')
                elif(len(directoryContent) != 0):
                    filename_InDirectory = directoryContent.keys() # getting only the names of the files or other directory at this location.
                    df_temp = pd.DataFrame.from_dict(filename_InDirectory)
                    df_FB = pd.concat([df_FB, df_temp], ignore_index=True)
             
                #Cheking if the directory exists in MongoDB.
                    st.write('The contents of the Directory on Firebase:')
                    df_FB.columns = [filePath]
                    st.write(df_FB)
                else:
                    st.write('No contents in this directory on Firebase')

            with col4:
                ############## MongoDB ################
                #Creating DB Connection 
                client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
                db = client.proj #establish connection to the  db
                pathName = filePath.rsplit('/', 1)
                filePath = pathName[0]
                df_MDB = pd.DataFrame() # Creating an empty dataframe to store the contents of the Files.
                dataFetched = list(db.Data.find({filePath: {"$exists": True}}))
                if dataFetched == None:
                    st.write('No content in this directory on MongoDB')
                elif(len(dataFetched) != 0):
                    key = filePath.split("/")[-1]
                    if(len(dataFetched) != 0):
                        data = dataFetched[0][key]
                        filename_InDirectory = data.keys()
                        df_temp = pd.DataFrame.from_dict(filename_InDirectory)
                        df_MDB = pd.concat([df_MDB, df_temp], ignore_index=True)
             
                        #Cheking if the directory exists in MongoDB.
                        st.write('The contents of the Directory on Firebase:')
                        df_MDB.columns = [filePath+"/"]
                        st.write(df_MDB)    
                else:
                    st.write('No contents in this directory on MongoDb')

if __name__=="__main__":
    main()