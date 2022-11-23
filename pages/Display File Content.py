from fileinput import filename
from this import d
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'

def main():
    with st.form("my_form"):
        filePath = st.text_input('Enter the file name along with its path to view its content:', "eg: User/john/hello.txt")
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('Loading the data from the File System..')

        # Splitting the string to seperate the file name and the path which specifies the location of the file.
        pathList = filePath.rsplit('/', 1)
        pathToDirectory = pathList[0]
        #removing the "." extension from the file.
        fileNameWithExtension = pathList[1].rsplit('.',1)
        fileName = fileNameWithExtension[0]

        # Checking how many partitons the files has.
        numofPartitions = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/partitions.json").text)

        # Creating a set which contains the names of all the partitions of a File which has to be deleted
        names = {""}
        for i in range(1,numofPartitions+1):
           names.add(fileName+str(i))

        #Checking in which db the file is present
        dbName = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/dbname.json").text)

        if dbName == "FIREBASE":
            #print("Firebase")
            #Getting the Data of files from all the partitions
            df = pd.DataFrame() # Creating an empty dataframe to store the file.
            for i in range(1,numofPartitions+1):
                fileData = json.loads(requests.get(baseURL+"Data/"+pathToDirectory+"/"+fileName+str(i)+".json").text)
                print(fileData,"FILE CONTENT")
                #print(baseURL+"Data/"+pathToDirectory+fileName+str(i)+".json")
                json_object = json.dumps(fileData)
                df_temp = pd.read_json(json_object, orient ='records')
                df = pd.concat([df, df_temp], ignore_index=True)

            st.write('Data LOADED')
            st.write(df)     
        else:
            #print("MONGO DB SECTION")
            # Connecting to MongodB
            client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
            db = client.proj #establish connection to the  db
            
            #Getting the Data of files from all the partitions
            data = [] # will store all the partitions in this list
            for i in range(1,numofPartitions+1):
                #directory path to file 
                directories = pathToDirectory.split('/')
                #print(directories,"PATH")
                searchPath = directories[0]+"."+(".".join(directories[1:]))
                searchPath = searchPath+fileName+str(i)
                b = list(db.Data.find({},{'_id':0,searchPath:1}))
                for a in b:
                    if len(a) != 0:
                        Val = {}
                        for j in directories:
                            Val = a[j]
                        
                        data.append(Val[fileName+str(i)])    
                
                #print(data)
            

            df = pd.DataFrame() # Creating an empty dataframe to store the file.

            for i in data:
                # concatinating all the partitions into one df
                df_temp = pd.DataFrame(i)
                df = pd.concat([df,df_temp],ignore_index=True)
            

            st.write(df)
                




if __name__=="__main__":
    main()