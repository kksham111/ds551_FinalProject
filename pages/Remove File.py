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
        filePath = st.text_input('Enter the Path to the File to Remove it', "eg: User/john/hello.txt")
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('Removing the file from the File System...')
       
        ## Splitting the string the seperate the file name and the path which specifies the location of the file.
        pathList = filePath.rsplit('/', 1)
        pathToDirectory = pathList[0]
        #removing the "." extension from the file.
        fileNameWithExtension = pathList[1].rsplit('.',1)
        fileName = fileNameWithExtension[0]
 
        # Checking how many partitons the files has.
        numofPartitions = json.loads(requests.get(baseURL+"/FileLocations/"+fileName+"/partitions.json").text)
       
        # Creating a set which contains the names of all the partitions of a File which has to be deleted
        names = {""}
        for i in range(1,numofPartitions+1):
           names.add(fileName+str(i))

        #Checking in which db the file is present
        dbName = json.loads(requests.get(baseURL+"/FileLocations/"+fileName+"/dbname.json").text)
        if dbName == "FIREBASE":
            #get All the files from the Data section at the specified directory
            data = json.loads(requests.get(baseURL+"/Data/"+pathToDirectory+".json").text)
       
            #Create a new JSON data without including the files that has to be deleted.
            dataNew = {}
            for key in data:
            #adding the file to dataNew if doesn't have to be deleted.
               if(key not in names):
                 dataNew[key] = data[key]

   
            #data = {"universities_ranking_P copy1":1, "universities_ranking_P copy2":2}
            #adding rest of the data back after deleting 
            json_object = json.dumps(dataNew)
            response1 = requests.put(baseURL+'Data/User.json',json_object)
            #print(response1, response1)

            ############# REMOVING ALL THE METADATA RELATED TO THE FILE ##################
            
            # Getting all details of all the file from the  FileLocations Section.
            FileLocationsData = json.loads(requests.get(baseURL+"/FileLocations.json").text)
            #Create a new JSON data without including the files that has to be deleted.
            FL_dataNew = {}
            for key in FileLocationsData:
            #adding the file to FL_dataNew if doesn't have to be deleted.
               if(key not in names):
                 FL_dataNew[key] = FileLocationsData[key]
                        

            #adding rest of the data back after deleting 
            json_object2 = json.dumps(FL_dataNew)
            response2 = requests.put(baseURL+'FileLocations.json',json_object2)
            print(response2,"Response2")
          
            # Getting all details of all the file from the Metadata Section
            MetaData = json.loads(requests.get(baseURL+"Metadata.json").text)
            #print(baseURL+"MetaData.json")
            #print(MetaData,"METADATA")
            #Create a new JSON data without including the files that has to be deleted.
            MD_dataNew = {}
            for key in MetaData:
            #adding the file to MD_dataNew if doesn't have to be deleted.
               if(key not in names):
                 MD_dataNew[key] = MetaData[key]
            #adding rest of the data back after deleting 
            json_object3 = json.dumps(MD_dataNew)
            response3 = requests.put(baseURL+'Metadata.json',json_object3)
            #print(response3,"Response3")

            #Updating the Lastnode to reflects the correct amount of nodes in the File System.
            #At first, get the current value of the Lastnode
            lastNode_ID = int(json.loads(requests.get(baseURL+"Metadata/Lastnode.json").text))
            #subtract the number of nodes that are deleted and update the Lastnode value
            numDeleted = len(names)-1
            response4 = requests.patch(baseURL+"Metadata.json", '{"Lastnode":'+str(lastNode_ID-numDeleted)+'}')
            #print(response3,"Response4")

            st.write('File Successfully removed from the File System..!')
        else:
            # Connecting to MongodB
            client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
            db = client.proj #establish connection to the  db

            #finding the path to the File
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

            
            SearchPath = path[0]+"."+(".".join(path[1:]))
            for i in range(1,numofPartitions):
                searchPath = searchPath+fileName+str(i)


            DataObj = list(db.Data.find({},{"_id":0,"A1.B1":1}))
            ori = {"A1":{"B1":{"C1"}}}
            newObj = {"$set":{"A1":{"B1":""}}}
            db.Data.update_one(ori, newObj)


            print(DataObj)
            




if __name__=="__main__":
    main()