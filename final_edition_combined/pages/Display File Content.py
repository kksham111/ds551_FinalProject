from fileinput import filename
from this import d
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
def getFilesFromEDFS():
    AllFiles =json.loads(requests.get(baseURL+"FileLocations/.json").text)
    Keys = AllFiles.keys()
    FB_fileNames = []
    for i in Keys:
        #Building Directory of each Nodes.
        if i != "Count":
            FB_fileNames.append(i+".csv")
    return FB_fileNames

def main():
    with st.form("my_form"):
        FileNamesList = getFilesFromEDFS()
        #Select Box to select the file name
        fileOption = st.selectbox(
           'Select a file name',FileNamesList)

        submitted = st.form_submit_button("Submit")

    if submitted:
        placeholder = st.empty()

        fileName = fileOption.split(".",1)[0]

        #get the database location of file.
        dbName = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/dbname.json").text)
        
        # Checking how many partitons the files has.
        numofPartitions = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/partitions.json").text)

        # Creating a set which contains the names of all the partitions of a File which has to be deleted
        names = {""}
        for i in range(1,numofPartitions+1):
           names.add(fileName+str(i))

        #Checking in which db the file is present
        dbName = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/dbname.json").text)

        
        st.subheader("**Searching for "+fileName+".csv ....**")

        st.write("***File is located on"+dbName+"***")
        st.write("***"+fileName+".csv is stored as "+str(numofPartitions)+" three partitions.***")
        st.subheader("**Fetching path...**")

        if dbName == "FIREBASE":
            #Getting Path of File
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


            st.write("***The file is located at "+path+"***") 
            st.subheader("**Loading all partitions....**")  

            #Getting the Data of files from all the partitions
            df = pd.DataFrame() # Creating an empty dataframe to store the file.
            for i in range(1,numofPartitions+1):
                fileData = json.loads(requests.get(baseURL+"Data/"+path+fileName+str(i)+".json").text)
                #print(baseURL+"Data/"+pathToDirectory+fileName+str(i)+".json")
                json_object = json.dumps(fileData)
                df_temp = pd.read_json(json_object, orient ='records')
                df = pd.concat([df, df_temp], ignore_index=True)
            
            st.subheader("**Successfully loaded"+fileName+".csv..!**")
            st.write(df)     
        else:
            # Connecting to MongodB
            client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
            db = client.proj #establish connection to the  db
            
            # finding the path at which the file is located
            #getting all the entries in MetdaData Section
            Metadata = db["Metadata"]
            MetadataObj = list(Metadata.find({},{"_id":0}))
            # Storing all the keys from the MetaData into KeyList
            #print(cursor)
            KeyList = []
            for i in MetadataObj[1:]:
                tempList = list(i.keys())
                KeyList.append(tempList[0])   

            #search the Parent directory in the which the file located. 
            previousNode = list(db.Metadata.find({},{"_id":0,fileName+str(1)+".previousNode":1}))
            global prevdirectoryId
            for i in previousNode:
                if len(i) != 0:
                    prevdirectoryId = i[str(fileName)+"1"]['previousNode']
            
            #building path to file from root
            path = []
            while prevdirectoryId != 0:
                for k in KeyList:
                    pipeline = [{"$match": {k+".id":{"$eq":prevdirectoryId}}},{"$project":{"_id":0}}]
                    result = list(Metadata.aggregate(pipeline))
                    if len(result) != 0:
                        prevdirectoryId = int(result[0][k]["previousNode"])
                        path.append(k)

            #getting the data from the path
            searchPath = path[0]+"."+(".".join(path[1:]))
            filePath = searchPath 
            st.write("***The file is located at "+filePath+"***") 
            st.subheader("**Loading all partitions....**") 
            data = [] # will store all the partitions in this list
            
            for i in range(1,numofPartitions):
                searchPath = searchPath+fileName+str(i)
                b = list(db.Data.find({},{'_id':0,searchPath:1}))
                for a in b:
                    if len(a) != 0:
                        Val = {}
                        for j in path:
                            Val = a[j]
                        
                        data.append(Val[fileName+str(i)])   

            df = pd.DataFrame()
            for i in data:
                # concatinating all the partitions into one df
                df_temp = pd.DataFrame(i)
                df = pd.concat([df,df_temp],ignore_index=True) 
            
            st.write("File is located on "+dbName)
            st.write("File is located inside "+ filePath +" ")
            st.write(" Succcessfully Loaded the Partition ")
            st.write(df)




if __name__=="__main__":
    main()