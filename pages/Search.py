import streamlit as st
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient


baseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'

def filter(query, dataframe):
    df = dataframe[query]
    return df

def main():
# Getting all the files present the database for querying
    #FileNames = json.loads(requests.get(baseURL+"/FileLocations.json").text) 
    #CurrentFiles = list(FileNames.keys())
    #CurrentFiles.remove("Count")
    #cols = st.columns(3)

    #Loading____________universitsies_ranking_P______data 
    numOfPartitions = json.loads(requests.get(baseURL+"/FileLocations/universities_ranking_P/partitions.json").text)
    if 'Data1' not in st.session_state:
        st.session_state.Data1 = []
    
    
    if len(st.session_state.Data1) == 0:
        print("DATA")
        #find the file location
        #search the Parent directory in the which the file located. 
        previousNode = json.loads(requests.get(baseURL+"/Metadata/"+"universitsies_ranking_P"+"1/previousNode.json").text)
        path = ""

        while previousNode != 0:
            #get the parent directory
            previousNodeData = json.loads(requests.get(baseURL+'/Metadata.json?orderBy="id"&equalTo=1').text)
            #update the previousNode
            directoryName = list(previousNodeData.keys())[0]
            previousNode = previousNodeData[directoryName]['previousNode']
            path = directoryName+"/"

        for i in range(1,numOfPartitions+1):
            df = json.loads(requests.get(baseURL+"/Data/"+"".join(path)+"universities_ranking_P"+str(i)+".json").text) 
            json_object = json.dumps(df)
            df_temp = pd.read_json(json_object, orient ='records')
            st.session_state.Data1.append(df_temp)

        # FORM 1 - Find all the Universities that are ranked within the range Rank1 and Rank2:
    with st.form("my_form"):
        
        st.write("Find all the Universities that are ranked within the range Rank1 and Rank2:")
        if 'Result' not in st.session_state:
            st.session_state.Result = []

        col1, col2 = st.columns(2)
        with col1:
            Rank1 = st.text_input('Enter Rank 1')
            explain_Result = st.form_submit_button("Explain Result")
        with col2:
            Rank2 = st.text_input('Enter Rank 2')         
            submitted = st.form_submit_button("Submit")

        if submitted:
            st.session_state.Result = []
            # Retrieveing the universities_ranking_P file.

            #find the  number of partitons 
            #st.write("Data is stored as :"+str(numOfPartitions)+" partitions")
            #dataframe = pd.DataFrame()
            #query = (dataframe['ranking']<= int(Rank2) ) & (dataframe['ranking']>= int(Rank1))
            #result = []
            #for i in range(1,numOfPartitions):
            #    st.session_state.Result.append(filter(query,st.session_state.Data1[i]))
            
            def filterOnRank(dataframe):
                #df1 = dataframe[dataframe["ranking"] < 100].groupby('location')['location'].count()
                string = (dataframe["ranking"]<= int(Rank2) ) & (dataframe["ranking"]>= int(Rank1))
                print(string)
                df1 = dataframe[string]
                #st.write("PARTITION")
                #st.write(df1)
                return df1

            st.session_state.Result.append(list(map(filterOnRank(),st.session_state.Data1)))
            
            df_Final = pd.DataFrame() # Creating an empty dataframe to store the file.
            for i in st.session_state.Result[0]:
                df_Final = pd.concat([df_Final, i], ignore_index=True)
            
            df_res = df_Final[["ranking","title","location"]]
            st.write("Result:")
            st.write(df_res)
            
        if explain_Result:
            col1, col2 = st.columns(2)
            for i in range(len(st.session_state.Data1)):
                col1.write("Input (Partition "+str(i+1)+"):")
                col1.write(st.session_state.Data1[i])
                col2.write("result from Partition "+str(i+1))
                df_list = st.session_state.Result[0]
                df = df_list[i]
                df_res = df[["ranking","title","location"]]
                col2.write(df_res)

            st.session_state.Result = []

    
    #FORM 2 - Number of universities in each country having rank betwwen 20 and 30.
    with st.form("my_form_2"):
        st.write("Find the number of Universities in each country ranked within the range Rank1 and Rank2:")
        if 'Result2' not in st.session_state:
            st.session_state.Result2 = []
        
        col1, col2 = st.columns(2)
        with col1:
            Rank1 = st.text_input('Enter Rank 1')
            explain_Result = st.form_submit_button("Explain Result")
        with col2:
            Rank2 = st.text_input('Enter Rank 2')         
            submitted = st.form_submit_button("Submit")
            if submitted:
                st.session_state.Result2 = []
                # Retrieveing the universities_ranking_P file.

                #find the  number of partitons 
                st.write("Data is stored as :"+str(numOfPartitions)+" partitions")

                result = []
                def filterByCountry(dataframe):
                    df1 = dataframe[(dataframe["ranking"]<= int(Rank2) ) & (dataframe["ranking"]>= int(Rank1))].groupby('location')['location'].count()
                    st.write(df1)
                    return df1

                for i in numOfPartitions:
                    st.session_state.Result2.append(filterByCountry(st.session_state.Data1[i]))

                #Displaying the Result.
                df_Final = pd.DataFrame() # Creating an empty dataframe to store the file.
                for i in st.session_state.Resul2[0]:
                    df_Final = pd.concat([df_Final, i], ignore_index=True)
            
                st.write("Result:")
                st.write(df_Final)

                

            
            #df_Final = pd.DataFrame() # Creating an empty dataframe to store the file.
            #for i in st.session_state.Result[0]:
            #    df_Final = pd.concat([df_Final, i], ignore_index=True)
            
            #df_res = df_Final[["ranking","title","location"]]
            #st.write("Result:")
            #st.write(df_res)
            
        #if explain_Result:
        #    col1, col2 = st.columns(2)
        #    for i in range(len(st.session_state.Data1)):
        #        col1.write("Input (Partition "+str(i+1)+"):")
        #        col1.write(st.session_state.Data1[i])
        #        col2.write("result from Partition "+str(i+1))
        #        df_list = st.session_state.Result[0]
        #        df = df_list[i]
        #        df_res = df[["ranking","title","location"]]
        #        col2.write(df_res)

        #    st.session_state.Result = []

        

        
        
            



if __name__=="__main__":
     main()