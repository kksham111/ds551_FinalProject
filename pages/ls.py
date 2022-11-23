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

def main():
    with st.form("my_form"):
        filePath = st.text_input('Enter a vlaid directory to list its content:', "eg: User/john")
        submitted = st.form_submit_button("Submit")

    if submitted:
        st.write('Retieveing the contents of the directory...')
        
        ############## FireBase ################
        df = pd.DataFrame() # Creating an empty dataframe to store the contents of the Files.
        #Cheking if the directory exists in Firebase.
        directoryContent = json.loads(requests.get(baseURL+"Data/"+filePath+".json").text)
        
        if(len(directoryContent) != 0):
            filename_InDirectory = directoryContent.keys() # getting only the names of the files or other directory at this location.
            df_temp = pd.DataFrame.from_dict(filename_InDirectory)
            df = pd.concat([df, df_temp], ignore_index=True)
             
           #Cheking if the directory exists in MongoDB.
            st.write('The contents of the Directory on Firebase:')
            df.columns = [filePath+"/"]
            st.write(df)
        else:
            st.write('No contents in this directory on Firebase')

        ############## MongoDB ################
        #Creating DB Connection 
        client = MongoClient("mongodb+srv://DSCI551Project:Dsci551@cluster0.2sh73fu.mongodb.net/?retryWrites=true&w=majority")
        db = client.proj #establish connection to the  db
        
        df = pd.DataFrame() # Creating an empty dataframe to store the contents of the Files.
        dataFetched = list(db.Data.find({filePath: {"$exists": True}}))

        if(len(directoryContent) != 0):
            key = filePath.split("/")[-1]
            if(len(dataFetched) != 0):
                data = dataFetched[0][key]
                filename_InDirectory = data.keys()
                print(filename_InDirectory)
                df_temp = pd.DataFrame.from_dict(filename_InDirectory)
                df = pd.concat([df, df_temp], ignore_index=True)
             
                #Cheking if the directory exists in MongoDB.
                st.write('The contents of the Directory on Firebase:')
                df.columns = [filePath+"/"]
                st.write(df)    
        else:
            st.write('No contents in this directory on MongoDb')


        







if __name__=="__main__":
    main()