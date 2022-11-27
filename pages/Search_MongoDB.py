import streamlit as st
import requests
import json
import pandas as pd
import pymongo
import re
from pymongo import MongoClient


def clean_paranthesis(str):
    return re.sub('"', "", str)


def clean_perc(str):
    a = float(re.sub('%', "", str))
    return a


def mapPartition(data, dataname, partition_num, key, sort, value):
    # data is divided into certain partitions
    # mapPartition() read data and take a partition as input
    # return a map(key:value) unsorted

    dataname_p = dataname+partition_num
    data_p = data[dataname_p]
    map_list = []

    for i in range(len(data_p)):
        key_i = data_p[i][key]  # country name
        sort_i = data_p[i][sort]
        return_i = [key_i, sort_i]

        for item in value:
            if item == "Literacy":
                aaa = clean_paranthesis(data_p[i][item])
                return_i.append(aaa)
            elif item not in data_p[i].keys():
                return_i.append('null')
            else:
                return_i.append(data_p[i][item])
        map_list.append(return_i)
    return map_list


def reducer(input_map, option, queries, multiselect):
    # input_map: Country, Sort, Value1, Value2...
    # read in map for each partition
    # output reduced
    return_lst = []

    if option == 'option_1':
        input_map.sort(key=lambda x: x[1], reverse=True)
        return_lst = input_map[0:queries]
    if option == 'option_2':
        target_year = queries[0]
        target_gdp = queries[1]
        for i in range(len(input_map)):
            if input_map[i][1] == target_year:
                input_map[i][1] = int(input_map[i][1])
                if input_map[i][2] > target_gdp:
                    format(input_map[i][2], '.1f')
                    return_lst.append(input_map[i])

    return return_lst


def search_country(databaseURL):
    input_11 = st.text_input("Number of countries to return(max: 228): ",  key="M_op1_in_1")

    client = MongoClient(databaseURL)
    db = client["proj"]
    return_obj = list(db.Data.find({'CountryDataP': {'$exists': True}}))[0]
    return_data = return_obj['CountryDataP']

    input_12 = st.text_input("Enter attributes you want to sort with:", key="M_op1_in_2")
    if input_12 not in return_data['CountryDataP1'][0].keys():
        st.warning("Please enter valid attributes.")

    input_13 = st.text_input("Enter additional attributes to show in the result:", key="M_op1_in_3")
    if input_13 not in return_data['CountryDataP1'][0].keys():
        st.warning("Please enter valid attributes.")

    button_Mon_o1 = st.button("Continue", key="continue_Mon_o1")

    if button_Mon_o1:
        input_11 = st.session_state["M_op1_in_1"]
        num = int(input_11)
        input_12 = st.session_state["M_op1_in_2"]
        input_13 = st.session_state["M_op1_in_3"]
        map1 = mapPartition(return_data, 'CountryDataP', "1", 'Country',  input_12, [input_13])
        map2 = mapPartition(return_data, 'CountryDataP', "2", 'Country',  input_12, [input_13])
        map3 = mapPartition(return_data, 'CountryDataP', "3", 'Country',  input_12, [input_13])
        map_all = map1 + map2 + map3
        reduced = reducer(map_all, 'option_1', queries=num, multiselect=None)

        title = ['Country'] + [input_12] + [input_13]
        output_dataframe = pd.DataFrame(reduced, columns=title).head(num)
        st.write("The following dataframe shows the top ", num, "countries in ", input_12, ":")
        st.write(output_dataframe)


def search_GDP(databaseURL):
    client = MongoClient(databaseURL)
    db = client["proj"]
    return_obj = list(db.Data.find({'GDPExpendenditure_in_percentage_R': {'$exists': True}}))[0]
    return_data = return_obj['GDPExpendenditure_in_percentage_R']

    input_21 = st.text_input("Enter a target percentage rate: ",  key="M_op2_in_1")
    input_22 = st.text_input("Enter a year for searching: ",  key="M_op2_in_2")

    button_Mon_o2 = st.button("Continue", key="continue_Mon_o2")

    if button_Mon_o2:
        input_21 = st.session_state["M_op2_in_1"]
        num = clean_perc(input_21)
        input_22 = st.session_state["M_op2_in_2"]
        year = int(input_22)

        map1 = mapPartition(return_data, 'GDPExpendenditure_in_percentage_R', "1", 'Country', 'Year', ['% of GDP'])
        map2 = mapPartition(return_data, 'GDPExpendenditure_in_percentage_R', "2", 'Country', 'Year', ['% of GDP'])
        map3 = mapPartition(return_data, 'GDPExpendenditure_in_percentage_R', "3", 'Country', 'Year', ['% of GDP'])
        map_all = map1 + map2 + map3

        reduced = reducer(map_all, 'option_2', [year, num], multiselect=None)
        reduced.sort(key=lambda x: x[2])   # highest comes first
        title = ['Country', 'Year', 'Percentage of GDP']
        output_dataframe = pd.DataFrame(reduced).head(10)
        output_dataframe[2] = output_dataframe[2].apply(lambda x: str(x) + '%')
        # output_dataframe[2].apply(lambda x: '%.2f%%' % x)
        output_dataframe.columns = title
        st.write("The following DataFrame shows countries that have percentage of GDP higher than", num, "% in ", year, ":")
        st.write(output_dataframe)


def main():
    st.title('Search in MongoDB')

    c_URL = st.container()
    databaseURL = "mongodb+srv://dsci551:dsci551@cluster0.djzvd82.mongodb.net/?retryWrites=true&w=majority"
    # databaseURL = c_URL.text_input('Please enter your database URL:  ')
    if databaseURL:
        c_URL.success('✔️ Success!')
    else:
        st.stop()

    c_options = st.container()
    with c_options:
        options_list = [" ", "Find TOP ... countries in...",
                        "Find GDP expenditure greater than ... in year..."]
        option_selectbox = st.selectbox("Please choose one searching options down below", options_list
                                        )
        if option_selectbox == options_list[0]:
            st.stop()
        if option_selectbox == options_list[1]:
            search_country(databaseURL)
        if option_selectbox == options_list[2]:
            search_GDP(databaseURL)




















if __name__=="__main__":
    main()
