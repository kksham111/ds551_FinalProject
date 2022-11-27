import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

# baseURL = 'https://filesystem-58643-default-rtdb.firebaseio.com/'
baseURL2 = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'


def checkDirectory(directories):
    # Iterating from 1st position and checking if the directory already exixts.
    # if not, the creating the directory.
    path = ""
    prevID = 0
    for directory in directories[1:]:
        list = json.loads(requests.get(baseURL2 + "/Metadata/" + directory + ".json").text)
        if (list == None):
            # MetaData
            # getting the ID of the LastNode.
            # print(prevID,"INITIAL PREVID INSIDE IF")
            lastNode_ID = int(json.loads(requests.get(baseURL2 + "/Metadata/Lastnode.json").text))
            res1 = requests.put(baseURL2 + "/Metadata/" + directory + "/.json",
                                '{"id":' + str(lastNode_ID + 1) + ', "type": "DIRECTORY","previousNode":' + str(
                                    prevID) + '}')
            # updating PrevID
            prevID = lastNode_ID + 1
            # print(prevID,"FINAL PREV ID INSIDE ELSE")
            # update the last node
            res2 = requests.patch(baseURL2 + "/Metadata.json", '{"Lastnode":' + str(lastNode_ID + 1) + '}')
            # creating the directory in data section
            res3 = requests.patch(baseURL2 + "/Data" + path + ".json", '{"' + str(directory) + '":""}')
            # print(baseURL2+"/Data"+path+".json")
            path += "/" + directory
            # print(res1,res2,res3,"RES1&2&3")
        else:
            prevID = list["id"]
            path = "/" + directory
            ##--have to update path of directory exists.
    return prevID


# Function to create the directory path in the Metadata and Data nodes of the MongoDb
def checkDirectory_MongoDb(directories, db, fileName, dataframe, columnName, partitions):
    pathDict = {}
    pathDictFull = pathDict
    prevID = 0

    for directory in directories[1:]:
        dataFetched = list(db.Metadata.find({directory: {"$exists": True}}))
        # Getting lastnode value
        # if the directly exists  dont create a new one else create the directory.
        if len(dataFetched):
            # DIRECTORY EXISTS
            # get the Id Value of the Node and update the prevID
            prevIDDict = list(db.Metadata.find({}, {"_id": 0, str(directory) + ".id": 1}))
            for d in prevIDDict:
                if len(d) != 0:
                    prevID = d[str(directory)]["id"]

            # getting the Contents of this Directory.
            currentDirectoryContents = list(db.Data.find({}, {str(directory): 1}))
            currDict = currentDirectoryContents[0]
            # --print(type(currentDirectoryContents))
            # --print(currentDirectoryContents)
            # Updating pathDict
            pathDict[str(directory)] = currDict
            pathDict = pathDict[str(directory)]

        else:
            # DIRECTORY DOESN'T EXISTS

            # Getting the value of the lastnode
            LastnodeDict = db.Metadata.find_one({}, {'_id': 0, 'Lastnode': 1})

            # Adding meta data about the directory in Metadata section
            dictMetadata = {directory: {"id": str(int(LastnodeDict["Lastnode"]) + 1), "type": "DIRECTORY",
                                        "previousNode": str(prevID)}}
            resM = db.Metadata.insert_one(dictMetadata)
            # print(resM,"UPDATE RESULT M")

            # Creating the path dict for the data section
            pathDict[str(directory)] = {}
            pathDict = pathDict[str(directory)]

            # updating PrevID so we can trace the path
            prevID = int(LastnodeDict["Lastnode"]) + 1

            # Updating the value of the Lastnode
            oldval = {"Lastnode": LastnodeDict["Lastnode"]}
            newval = {"$set": {"Lastnode": str(int(LastnodeDict["Lastnode"]) + 1)}}
            db.Metadata.update_one(oldval, newval)

    # Updating the data node
    dataFetched = list(db.Data.find({directories[1]: {"$exists": True}}))
    if len(dataFetched):
        res = db.Data.delete_one(dataFetched[0])

        # Entering Partition dataset
        keys = partitions.keys()
        for i in keys:
            r = (dataframe[columnName] >= partitions[i][0]) & (dataframe[columnName] <= partitions[i][1])
            pathDict[fileName + str(i + 1)] = dataframe[r].to_dict("records")

        resD = db.Data.insert_one(pathDictFull)
    else:
        # Entering Partition dataset
        keys = partitions.keys()
        for i in keys:
            r = (dataframe[columnName] >= partitions[i][0]) & (dataframe[columnName] <= partitions[i][1])
            pathDict[fileName + str(i + 1)] = dataframe[r].to_dict("records")
        resD = db.Data.insert_one(pathDictFull)

    return pathDictFull, prevID


def main():
    # retrieve the number of files uploaded till Now.
    uploadedFileCount = json.loads(requests.get(baseURL2 + "/FileLocations/Count.json").text)
    print("Count of Uploaded File:", uploadedFileCount)

    # add the key choices_len to the session_state
    if not "choices_len" in st.session_state:
        st.session_state["choices_len"] = 0

    # c_up contains the form
    # c_down contains the add and remove buttons
    c_up = st.container()
    c_down = st.container()

    with c_up:
        with st.form("myForm"):
            # Input for directory path
            filePath = st.text_input('Enter the directory/path for storing the file:', '/', key="FILEPATH")
            # Input for csv file
            uploaded_file = st.file_uploader("Choose a file")

            # Range Partition:
            st.markdown("Partition Method: RANGE PARTITION (add ranges)")
            # The Coulmn name to create partitions
            columnName = st.text_input('Column Name for partitions:', key="CoumnName")

            c1 = st.container()  # c1 contains choices
            c2 = st.container()  # c2 contains submit button
            with c2:
                submitted = st.form_submit_button("submit")

    with c_down:
        col_l, _, col_r = st.columns((4, 15, 4))
        with col_l:
            if st.button("Add Range"):
                st.session_state["choices_len"] += 1

        with col_r:
            if st.button("remove Range") and st.session_state["choices_len"] > 1:
                st.session_state["choices_len"] -= 1
                st.session_state.pop(f'{st.session_state["choices_len"]}')

    for x in range(st.session_state["choices_len"]):  # create many choices
        with c1:
            st.text_input("Range:", key=f"{x}")

    if submitted:
        if filePath == None or uploaded_file == None or columnName == None:
            st.error("Error - Refresh the page and enter data again")
        else:

            directories = filePath.split("/")

            # Num of Partitions
            partitions = {}
            numOfPartitions = 0
            for x in range(st.session_state["choices_len"]):
                partitions[x] = st.session_state[f"{x}"].split("-")
                st.markdown(st.session_state[f"{x}"])
                numOfPartitions += 1

            ## Processing the  CSV file   
            dataframe = pd.read_csv(uploaded_file)
            # Partitioning the dataset
            try:
                dataframe = dataframe.sort_values(columnName)
            except:
                st.error("Refresh and Try again. Column name is not present in the csv file.")
            joinedStr = "/".join(directories[1:])
            data = []

            # Update the count of the files that have been uploaded.
            updateCounts = requests.patch(baseURL2 + "/FileLocations/.json",
                                          '{"Count":' + str(uploadedFileCount + 1) + '}')
            fileName = uploaded_file.name.split(".")

            # Storing even files in Firebase and odd files in mongoDB
            if uploadedFileCount % 2 == 0:
                #####################ADD FILE TO FIREBASE####################               

                # CHECK IF DIRECTORY EXISTS
                st.write("File successfully uploaded to ", filePath)
                prevID = checkDirectory(directories)

                # Num of Partitions
                # partitions = {}
                # numOfPartitions = 0
                # for x in range(st.session_state["choices_len"]):
                #    partitions[x] =  st.session_state[f"{x}"].split("-")
                #    st.markdown(st.session_state[f"{x}"])
                #    numOfPartitions+=1

                # updating the location of database where file is stored. (Contains which database and how many partitions the file is stored in)
                fileDBLoc = requests.patch(baseURL2 + "/FileLocations/" + fileName[0] + ".json",
                                           '{"dbname":"FIREBASE", "partitions":' + str(numOfPartitions) + '}')

                ### CSV file
                # dataframe = pd.read_csv(uploaded_file)
                ##Partitioning the dataset
                # dataframe = dataframe.sort_values(columnName)
                # joinedStr = "/".join(directories[1:])
                # data = []

                ##Create partitions and store metadata about the partitions
                keys = partitions.keys()
                for i in keys:
                    r = (dataframe[columnName] >= partitions[i][0]) & (dataframe[columnName] <= partitions[i][1])
                    data.append(dataframe[r])
                    # MetaData
                    # getting the ID of the LastNode.
                    lastNode_ID = int(json.loads(requests.get(baseURL2 + "/Metadata/Lastnode.json").text))
                    res1 = requests.patch(baseURL2 + "/Metadata/" + fileName[0] + str(i + 1) + ".json",
                                          '{"id":' + str(lastNode_ID + 1) + ', "type": "FILE","previousNode":' + str(
                                              prevID) + '}')
                    ##update the last node
                    res2 = requests.patch(baseURL2 + "/Metadata/.json", '{"Lastnode":' + str(lastNode_ID + 1) + '}')

                    ##Data
                    res4 = requests.patch(baseURL2 + "Data/" + joinedStr + "/.json",
                                          '{"' + fileName[0] + str(i + 1) + '":' + dataframe[r].to_json(
                                              orient="records") + '}')
                    st.markdown("Partition" + str(i + 1))
                    st.write(dataframe[r])
                    # print(dataframe[r])

                    # print(data);
                    # print(len(data))

            else:
                ##################### ADD FILE TO MONGODB ####################
                # updating the location info on Firebase.
                fileDBLoc = requests.patch(baseURL2 + "/FileLocations/" + fileName[0] + ".json",
                                           '{"dbname":"MONGODB", "partitions":' + str(numOfPartitions) + '}')

                # Connecting to MongoDB
                # client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
                client = MongoClient(
                    "mongodb+srv://dsci551:dsci551@cluster0.djzvd82.mongodb.net/?retryWrites=true&w=majority")
                db = client.proj  # establish connection to the  db
                keys = partitions.keys()
                dataPath, prevID = checkDirectory_MongoDb(directories, db, fileName[0], dataframe, columnName,
                                                          partitions)

                ##Create partitions and store metadata about the partitions
                for i in keys:
                    r = (dataframe[columnName] >= partitions[i][0]) & (dataframe[columnName] <= partitions[i][1])
                    data.append(dataframe[r])

                    # MetaData
                    # getting the ID of the LastNode.
                    LastnodeDict = db.Metadata.find_one({}, {'_id': 0, 'Lastnode': 1})

                    # Adding metadata about the partition
                    dictMetadata = {
                        str(fileName[0] + str(i + 1)): {"id": str(int(LastnodeDict["Lastnode"]) + 1), "type": "FILE",
                                                        "previousNode": str(prevID)}}
                    resM = db.Metadata.insert_one(dictMetadata)

                    # Updating the value of the Lastnode
                    oldval = {"Lastnode": LastnodeDict["Lastnode"]}
                    newval = {"$set": {"Lastnode": str(int(LastnodeDict["Lastnode"]) + 1)}}
                    db.Metadata.update_one(oldval, newval)

                    ##display partitions on screen
                    st.markdown("Partition" + str(i + 1))
                    st.write(dataframe[r])


# st.sidebar.markdown("# Content 2 🎈")


if __name__ == "__main__":
    main()
