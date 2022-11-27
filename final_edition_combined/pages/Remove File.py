from fileinput import filename
import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

baseURL = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'

def getFilesFromEDFS():
    fireBaseDirectories =json.loads(requests.get(baseURL+"FileLocations/.json").text)
    Keys = fireBaseDirectories.keys()
    FB_fileNames = []
    for i in Keys:
        #Building Directory of each Nodes.
        if i != "Count" and i != "GDPExpendenture_in_percentage" and i != "universisties_scores" and i != "universities_ranking_P":
            FB_fileNames.append(i+".csv")
    return FB_fileNames

def main():
    with st.form("my_form"): 
        FileNamesList = getFilesFromEDFS()
        option = st.selectbox(
           'Select a file name to Remove it',FileNamesList)
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('You selected:',option)
        lst = option.split(".",1)
        fileName = lst[0]
        placeholder = st.empty()
        with placeholder:
            st.write("***Removing  File...***")
        
        ######## Check the database Location of the file #############
        dbName =json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/dbname.json").text)
        numofPartitions = json.loads(requests.get(baseURL+"FileLocations/"+fileName+"/partitions.json").text)
        if dbName == "FIREBASE":
            ########## Delete file from Firebase ###############
            with placeholder:
                st.write("***Removing  File...***")
                st.write("This File is located on Firebase.")

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
            
            names = {""}
            with placeholder:
                st.write("***Removing  File...***")
                st.write("This File is located on Firebase.")
                st.write("The File is stored as "+str(numofPartitions)+"partitions")
                st.write("Deleting Partitions :"); 
                for i in range(1,numofPartitions+1):
                    st.write("Partition "+str(i)+":"+path+"/"+fileName+str(i))
                    names.add(fileName+str(i))
           
            ####### Deleting file from DataNode #########
            #Get All the files from the Data section at the specified directory
            data = json.loads(requests.get(baseURL+"/Data/"+path+".json").text)

       
            #Create a new JSON data without including the files that has to be deleted.
            dataNew = {}
            for key in data:
            #adding the file to dataNew if doesn't have to be deleted.
               if(key not in names):
                    dataNew[key] = data[key]
            if(len(dataNew) == 0):
                directories = path.split("/")
                directories = ' '.join(directories).split()
                if(len(directories) > 1):
                    pathStr = "/".join(directories[1:-1])
                    pathStr = directories[0]+pathStr
                    response1 = requests.patch(baseURL+'Data/'+pathStr+'.json','{"'+directories[len(directories) -1]+':""}')
                else:
                    response1 = requests.patch(baseURL+'Data/.json','{"'+directories[0]+'":""}')
            else:
                directories = path.split("/")
                directories = ' '.join(directories).split()
                json_object = json.dumps(dataNew)
                if(len(directories) > 1):
                    pathStr = "/".join(directories[1:-1])
                    pathStr = directories[0]+pathStr
                    response1 = requests.patch(baseURL+'Data/'+pathStr+'.json','{"'+directories[len(directories)-1]+'":'+json_object+'}')
                else:
                    #Directory is present directly under DataNode
                    response1 = requests.patch(baseURL+'Data/.json','{"'+directories[0]+'":'+json_object+'}')

            with placeholder:
                st.write("***Removing  File...***")
                st.write("This File is located on Firebase.")
                st.write("The File is stored as "+numofPartitions+"partitions")
                st.write("Deleting Partitions :"); 
                for i in range(1,numofPartitions+1):
                    st.write("Partition "+str(i)+":"+path+"/"+fileName+str(i))
                st.write("Removing the Metadata asssociated with the file...")
            
            #UPDATING FileLocations Section
            FileLoc = json.loads(requests.delete(baseURL+"/FileLocations/"+str(fileName)+".json").text)
            #UPDATING the Counts variable under the FileLocation section.
            Counts = json.loads(requests.get(baseURL+"/FileLocations/Count.json").text)
            res_Counts = requests.patch(baseURL+'FileLocations/.json','{"Count":'+str(Counts-1)+'}')
            
            #UPDATING THE MetaData Section
            #removing data about each partition
            for i in range(1,numofPartitions+1):
                FileLoc = json.loads(requests.delete(baseURL+"/Metadata/"+str(fileName)+str(i+1)+".json").text)

            with placeholder:
                st.write("File Successfully removed.!")
            ## Extracting the Location of the file.

            getFilesFromEDFS()

        elif dbName == "MONGODB":
            ########## Delete file from MongoDB ###############
            with placeholder:
                st.write("***Removing  File...***")
                st.write("This file is located on MongoDB.")

            #Connecting to MongoDB
            client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
            db = client.proj #establish connection to the  db
            ####### FIND THE PATH OF THE FILE #########
                        # finding the path at which the file is located
            #getting all the entries in MetdaData Section
            Metadata = db["Metadata"]
            MetadataObj = list(Metadata.find({},{"_id":0}))
            cursor = list(MetadataObj)
            
            ###Storing all the keys from the MetaData into KeyList
            KeyList = []
            for i in cursor[1:]:
                tempList = list(i.keys())
                KeyList.append(tempList[0]) 
                
            ###search the Parent directory in the which the file located. 
            previousNode = list(db.Metadata.find({},{"_id":0,fileName+str(1)+".previousNode":1}))
            global prevdirectoryId
            for i in previousNode:
                if len(i) != 0:
                    prevdirectoryId = i[str(fileName)+"1"]['previousNode']
            
            path = []
            while prevdirectoryId != 0:
                for k in KeyList:
                    pipeline = [{"$match": {k+".id":{"$eq":str(prevdirectoryId)}}},{"$project":{"_id":0}}]
                    result = list(Metadata.aggregate(pipeline))
                    if len(result) != 0:
                        prevdirectoryId = int(result[0][k]["previousNode"])
                        path.append(k)  

            path.reverse()
            Search_Str = ""
            if(len(path) > 1):
                Search_Str = ".".join(path[1:])
                Search_Str = path[0]+"."+Search_Str 
            else:
                Search_Str = path[0]

            #### List of File names that has to deleted
            delFiles = []
            for  part in range(1,numofPartitions+1):
                delFiles.append(fileName+str(part))
            ### Extracting all the data at this path####
            b = list(db.Data.find({},{'_id':0,str(Search_Str):1}))
            original = list(db.Data.find({},{'_id':0,str(Search_Str):1}))

            with placeholder:
                st.write("***Removing  File...***")
                st.write("This file is located on MongoDB")
                st.write("The File is stored as "+str(numofPartitions)+"partitions")
                st.write("Deleting Partitions :"); 
                for i in range(1,numofPartitions+1):
                    st.write("Partition "+str(i)+":"+Search_Str+"/"+fileName+str(i))
                st.write("Removing the Metadata asssociated with the file...")
            
            data = {}
            for i in b:
                if len(i) != 0:
                    data = i.copy()
                    break

            ### Deleting the Required Files
            sub = data
            for a in path:
                sub = sub[a]

            for b in range(1,numofPartitions+1):
                del sub[fileName+str(b)]

            data = data[str(path[0])]

            ### 
            originalDB = {}
            for i in original:
                if len(i) != 0:
                    originalDB = i.copy()
                    break

            ###Upading the data Node
            ###st.write({'"'+path[0]+'"':data},"CHECK")
            newval = { "$set":{path[0]:data}}
            res = db.Data.update_one(originalDB, newval)

            for i in range(1,numofPartitions+1):
                res1 = list(db.Metadata.find({},{'_id':1,fileName+str():1}))
                for j in res1:
                    if(len(j) >1):
                        id = j["_id"]
                        result = db.Metadata.delete_one({'_id': ObjectId(id)})
            
            ### Updating the FileLocation Node in Firebase:
            FileLoc = json.loads(requests.delete(baseURL+"/FileLocations/"+str(fileName)+".json").text)
            #UPDATING the Counts variable under the FileLocation section.
            Counts = json.loads(requests.get(baseURL+"/FileLocations/Count.json").text)
            res_Counts = requests.patch(baseURL+'FileLocations/.json','{"Count":'+str(Counts-1)+'}')
            
            with placeholder:
                st.write("**File Successfully removed.!**")
                st.write("**Refresh the Page before removing next file..!!**")
            
            getFilesFromEDFS()

if __name__=="__main__":
    main()