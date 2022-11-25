import streamlit as st
import requests
import json
import pandas as pd


def dict_add(dict, key, value):
    if key in dict.keys():
        tmp = dict[key]
        tmp.append(value)
        dict[key] = tmp
    else:
        dict[key] = [value]
    return dict


def mapPartition(baseURL, dataset, partition, key, value):
    # data is divided into certain partitions
    # mapPartition() read data and take a partition as input
    # return a map(key:value) unsorted
    data = json.loads(requests.get(baseURL + "Data" + "/" + dataset + "/" + dataset + partition + "/.json").text)

    map_list = []

    for i in range(len(data)):
        key_i = data[i][key]
        ranking_i = data[i]['ranking']

        return_i = []
        if value is not None:
            for item in value:
                return_i.append(data[i][item])
        map_list.append((ranking_i, key_i, return_i))
    return map_list


def reducer(input_map, option, queries):
    # read in map for each partition
    # output reduced
    return_lst = []
    if option == 'option_1':
        lower_num = queries[1]
        upper_num = queries[2]
        for item in input_map:
            if lower_num <= int(item[1]) <= upper_num:
                return_lst.append(item)
    return return_lst


def list_combiner(list1, list2, list3):
    return list1 + list2 + list3


def search_ranking(databaseURL):
    c_1, c_2 = st.columns(2)
    with c_1:
        lower_num = st.number_input("Enter lower boundary for ranking: ", min_value=1, max_value=1526)
    with c_2:
        upper_num = st.number_input("Enter upper boundary for ranking: ", min_value=1, max_value=1526)

    if lower_num and upper_num:
        if lower_num > upper_num:
            st.info("Please enter valid searching range.")
            st.stop()
    else:
        st.stop()

    map1 = mapPartition(databaseURL, "universities_ranking", "1", "ranking", ['title'])
    map2 = mapPartition(databaseURL, "universities_ranking", "2", "ranking", ['title'])
    map3 = mapPartition(databaseURL, "universities_ranking", "3", "ranking", ['title'])

    satisfied_1 = reducer(map1, 'option_1', ['ranking', lower_num, upper_num])
    satisfied_2 = reducer(map2, 'option_1', ['ranking', lower_num, upper_num])
    satisfied_3 = reducer(map3, 'option_1', ['ranking', lower_num, upper_num])

    return_list = list_combiner(satisfied_1, satisfied_2, satisfied_3)
    return_list.sort(key=lambda x: x[1])
    st.dataframe(return_list)
    # data = json.loads(requests.get(baseURL+".json").text)
    # data_P1 = data['Data']['User']['universities_ranking_P1']
    # data_P2 = data['Data']['User']['universities_ranking_P2']
    # data_P1.sort(key=lambda k: (k.get('ranking', 0)))
    # data_P2.sort(key=lambda k: (k.get('ranking', 0)))

    # i,j = 0, 0
    # return_list = []
    # ranking_i = data_P1[i]['ranking']
    # ranking_j = data_P2[j]['ranking']
    # while ranking_i < lower_num:
    #     i += 1
    #     ranking_i = data_P1[i]['ranking']
    # while ranking_j < lower_num:
    #     j += 1
    #     ranking_j = data_P2[j]['ranking']
    # while ranking_j <= upper_num or ranking_i <= upper_num:
    #     if ranking_j < ranking_i:
    #         return_list.append(data_P2[j])
    #         j += 1
    #         ranking_j = data_P2[j]['ranking']
    #     else:
    #         return_list.append(data_P1[i])
    #         i += 1
    #         ranking_i = data_P1[i]['ranking']


def search_name(databaseURL):

    return


def main():
    st.title('Search in Firebase')

    c_URL = st.container()
    databaseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'
    # databaseURL = c_URL.text_input('Please enter your database URL:  ')
    # if databaseURL:
    #     c_URL.info('✔️ Succeed!')
    # else:
    #     st.stop()

    ############################################# TEST

    #############################################

    c_options = st.container()
    with c_options:
        options_list = [" ", "Find universities ranking within certain range...",
                                     "Find universities name start with..."]
        option_selectbox = st.selectbox("Please choose one searching options down below", options_list
                                    )
        if option_selectbox == options_list[0]:
            st.stop()
        if option_selectbox == options_list[1]:
            search_ranking(databaseURL)
        if option_selectbox == options_list[2]:
            search_name(databaseURL)




if __name__ == "__main__":
    main()
