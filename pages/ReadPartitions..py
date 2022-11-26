from fileinput import filename
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'

def main():
    with st.form("my_form"):
        fileName = st.text_input('Enter the File Name')
        partitionNumber = st.text_input('Enter the Partition Number')
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('Searching for the partition'+partitionNumber+'of', fileName)

        #check in which database the file is present
        dbName = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/dbname.json").text)

        if dbName == "FIREBASE":
            #search the Parent direction in the which the file located. 
            previousNode = json.loads(requests.get(baseURL+"/Metadata/"+str(fileName)+"1/previousNode.json").text)
     
            path = ""
            while previousNode != 0:
                #get the parent directory
                previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo='+str(previousNode)).text)
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
        elif dbName == "MONGODB":
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
            searchPath = searchPath+fileName+str(partitionNumber)
            #print(searchPath)
            b = list(db.Data.find({},{'_id':0,searchPath:1}))


            #Extracting the data
            data = []
            for a in b:
                if len(a) != 0:
                    Val = {}
                    for j in path:
                        Val = a[j]
                        
                    data.append(Val[fileName+str(partitionNumber)])    
            
            df = pd.DataFrame()
            for i in data:
                # concatinating all the partitions into one df
                df_temp = pd.DataFrame(i)
                df = pd.concat([df,df_temp],ignore_index=True)
            st.write(df)

        else:
            st.write("FILE NOT PRESENT IN THE FILE SYSTEM")
       
       
       

if __name__=="__main__":
    main()
