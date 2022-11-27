import streamlit as st
import requests
import json
import pandas as pd
import pymongo
import re
from pymongo import MongoClient


def clean_paranthesis(str):
    return re.sub('"', "", str)


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
    # if option == 'option_2':
    #     quer_key = queries[0]
    #     for item in input_map:
    #         if quer_key == "location":
    #             index_loc = multiselect.index("location")
    #             if item[2][index_loc] == queries[1]:
    #                 return_lst.append(item)
    #         if quer_key == "perc intl students":
    #             index_intl = multiselect.index("perc intl students")
    #             intl_perc = queries[2]
    #             if queries[1] == "above":
    #                 if item[2][index_intl] > intl_perc:
    #                     return_lst.append(item)
    #             else:
    #                 if item[2][index_intl] < intl_perc:
    #                     return_lst.append(item)
    #         if quer_key == "number students":
    #             index_numstu = multiselect.index("number students")
    #             num_edge_stu = queries[2]
    #             if queries[1] == "above":
    #                 if item[2][index_numstu] > num_edge_stu:
    #                     return_lst.append(item)
    #             else:
    #                 if item[2][index_numstu] < num_edge_stu:
    #                     return_lst.append(item)

    return return_lst


def search_country(databaseURL):
    input_1 = st.text_input("Number of countries to return(max: 228): ",  key="M_op1_in_1")

    client = MongoClient(databaseURL)
    db = client["proj"]
    return_obj = list(db.Data.find({'CountryDataP': {'$exists': True}}))[0]
    return_data = return_obj['CountryDataP']

    input_2 = st.text_input("Enter attributes you want to sort with:", key="M_op1_in_2")
    if input_2 not in return_data['CountryDataP1'][0].keys():
        st.warning("Please enter valid attributes.")

    input_3 = st.text_input("Enter additional attributes to show in the result:", key="M_op1_in_3")
    if input_3 not in return_data['CountryDataP1'][0].keys():
        st.warning("Please enter valid attributes.")

    button_Mon_o1 = st.button("Continue", key="continue_Mon_o1")

    if button_Mon_o1:
        input_1 = st.session_state["M_op1_in_1"]
        num = int(input_1)
        input_2 = st.session_state["M_op1_in_2"]
        input_3 = st.session_state["M_op1_in_3"]
        map1 = mapPartition(return_data, 'CountryDataP', "1", 'Country',  input_2, [input_3])
        map2 = mapPartition(return_data, 'CountryDataP', "2", 'Country',  input_2, [input_3])
        map3 = mapPartition(return_data, 'CountryDataP', "3", 'Country',  input_2, [input_3])
        map_all = map1 + map2 + map3
        reduced = reducer(map_all, 'option_1', queries=num, multiselect=None)

        title = ['Country'] + [input_2] + [input_3]
        output_dataframe = pd.DataFrame(reduced, columns=title).head(num)
        st.write("The following dataframe shows the top ", num, "countries in ", input_2, ":")
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

    # client = MongoClient(databaseURL)
    # db = client["proj"]
    # data_collection = db.Data
    #
    # return_obj = list(data_collection.find({'CountryDataP': {'$exists': True}}))[0]
    # return_data = return_obj['CountryDataP']
    # map1 = mapPartition(return_data, 'CountryDataP', "1", 'Country', 'Population', ['Region', 'Area'])
    # map2 = mapPartition(return_data, 'CountryDataP', "2", 'Country', 'Population', ['Region', 'Area'])
    # map3 = mapPartition(return_data, 'CountryDataP', "3", 'Country', 'Population', ['Region', 'Area'])
    # map_all = map1+map2+map3
    #
    # reduced = reducer(map_all, 'option_1', 10, multiselect=None)




    c_options = st.container()
    with c_options:
        options_list = [" ", "Find TOP xxx countries in...",
                        "Find TOP universities with students staff ratio..."]
        option_selectbox = st.selectbox("Please choose one searching options down below", options_list
                                        )
        if option_selectbox == options_list[0]:
            st.stop()
        if option_selectbox == options_list[1]:
            search_country(databaseURL)
        # if option_selectbox == options_list[2]:
        #     search_students_staff_ratio(databaseURL)




















if __name__=="__main__":
    main()
