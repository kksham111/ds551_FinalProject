import streamlit as st
import requests
import json
import pandas as pd
import re


def toPercent(a):
    b = "%.1f%%" % (a * 100)
    return b


def dict_add(dict, key, value):
    if key in dict.keys():
        tmp = dict[key]
        tmp.append(value)
        dict[key] = tmp
    else:
        dict[key] = [value]
    return dict


def mapPartition(baseURL, dataset, partition_num, key, value):
    # data is divided into certain partitions
    # mapPartition() read data and take a partition as input
    # return a map(key:value) unsorted
    data = json.loads(requests.get(baseURL + "Data" + "/" + dataset + "/" + dataset + partition_num + "/.json").text)

    map_list = []

    for i in range(len(data)):
        key_i = data[i][key]
        ranking_i = data[i]['ranking']
        name_i = data[i]['title']
        return_i = []
        if value is not None:
            for item in value:
                if item == "perc intl students":
                    aaa = data[i][item]
                    if aaa == "%":
                        data[i][item] = 0
                    else:
                        data[i][item] = int(re.sub('%', "", aaa))/100

                if item == "number students":
                    aaa = data[i][item]
                    data[i][item] = re.sub(',',"", aaa)
                if item == "title":
                    aaa = data[i][item]
                    data[i][item] = re.sub('"',"", aaa)
                if item == "number students":
                    data[i][item] = int(data[i][item])
                return_i.append(data[i][item])
        map_list.append((key_i, name_i, return_i))
    return map_list


def reducer(input_map, option, queries, multiselect):
    # read in map for each partition
    # output reduced
    return_lst = []

    if option == 'option_1':
        lower_num = queries[1]
        upper_num = queries[2]
        for item in input_map:
            if lower_num <= int(item[0]) <= upper_num:
                return_lst.append([item[0],item[1]])

    if option == 'option_2':
        quer_key = queries[0]
        for item in input_map:
            if quer_key == "location":
                index_loc = multiselect.index("location") + 1
                if item[2][index_loc] == queries[1]:
                    return_lst.append(item)
            if quer_key == "perc intl students":
                index_intl = multiselect.index("perc intl students") + 1
                intl_perc = queries[2]
                if queries[1] == "above":
                    if item[2][index_intl] > intl_perc:
                        return_lst.append(item)
                else:
                    if item[2][index_intl] < intl_perc:
                        return_lst.append(item)
            if quer_key == "number students":
                index_intl = multiselect.index("number students") + 1
                num_edge_stu = queries[2]
                if queries[1] == "above":
                    if item[2][2] > num_edge_stu:
                        return_lst.append(item)
                else:
                    if item[2][2] < num_edge_stu:
                        return_lst.append(item)

    return return_lst


def list_combiner(list1, list2, list3):
    return list1 + list2 + list3


def search_ranking(databaseURL):
    c_num = st.container()
    with c_num:
        c_1, c_2 = st.columns(2)
        with c_1:
            lower_num = st.slider("Select lower boundary for ranking: ", min_value=0, max_value=1526)
        with c_2:
            upper_num = st.slider("Select upper boundary for ranking: ", min_value=0, max_value=1526)

        if lower_num == 0 and upper_num == 0:
            st.warning("Please select valid searching range.")
            st.stop()
        else:
            if lower_num > upper_num:
                st.warning("Please select valid searching range.")
                st.stop()

    map1 = mapPartition(databaseURL, "universities_ranking_P", "1", key="ranking", value=['title'])
    map2 = mapPartition(databaseURL, "universities_ranking_P", "2", key="ranking", value=['title'])
    map3 = mapPartition(databaseURL, "universities_ranking_P", "3", key="ranking", value=['title'])
    map_all = list_combiner(map1, map2, map3)

    satisfied = reducer(map_all, 'option_1', ['ranking', lower_num, upper_num], None)
    satisfied.sort(key=lambda x: x[0])

    c_out = st.container()
    with c_out:
        output_dataframe = pd.DataFrame(satisfied, columns=['Ranking', 'UniversityName'])
        st.dataframe(output_dataframe)
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


def search_students_staff_ratio(databaseURL):
    with st.form("input_option2"):
        num = st.slider("Select top xxx universities to return: ", min_value=0, max_value=20)
        multiselect_2 = st.multiselect("Additional Attributes options",
                                       options=("location", "perc intl students", "number students"))
        submitted_option2 = st.form_submit_button("Submit")

        if multiselect_2:
            f"Attributes selected: {'; '.join(i for i in multiselect_2)}。"

        if "location" in multiselect_2:
            country = st.text_input(label="Please input a country name:")
            if country:
                st.write("Results contain universities in", country, "only.")
        if "perc intl students" in multiselect_2:
            c_2_l, c_2_r = st.columns(2)
            with c_2_l:
                my_radio = st.radio(label=" ", key="intl_ratio", options=("above", "below"), label_visibility='hidden')
            with c_2_r:
                ratio_2 = st.number_input("Please enter a number", min_value=0, max_value=100, value=10)
                ratio_2_perc = ratio_2 / 100
            if my_radio and ratio_2:
                st.write("Results contain universities that have a international student ratio ", my_radio, ratio_2,
                         "%")
        if "number students" in multiselect_2:
            c_2_l2, c_2_r2 = st.columns(2)
            with c_2_l2:
                my_radio2 = st.radio(label=" ", key="number students", options=("above", "below"),
                                     label_visibility='hidden')
            with c_2_r2:
                ratio_22 = st.number_input("Please enter a number", min_value=0, value=10000)
            if my_radio2 and ratio_22:
                st.write("Results contain universities that have students number ", my_radio2, ratio_22)

        if submitted_option2:
            search_attributes = ['students staff ratio'] + multiselect_2
            map1 = mapPartition(databaseURL, "universities_ranking_P", "1", key="ranking", value=search_attributes)
            map2 = mapPartition(databaseURL, "universities_ranking_P", "2", key="ranking", value=search_attributes)
            map3 = mapPartition(databaseURL, "universities_ranking_P", "3", key="ranking", value=search_attributes)
            map_all = list_combiner(map1, map2, map3)

            # reduce with additional attributes first
            reduced = map_all
            if "location" in multiselect_2:
                red_loc = reducer(reduced, 'option_2', ["location", country])
                reduced = red_loc
            if "perc intl students" in multiselect_2:
                red_intl = reducer(reduced, 'option_2', ["perc intl students", my_radio, ratio_2_perc])
                reduced = red_intl
            if "number students" in multiselect_2:
                red_numStu = reducer(reduced, 'option_2', ["number students", my_radio2, ratio_22])
                reduced = red_numStu

            # then find the top xx universities with staff ratio
            reduced.sort(key=lambda x: x[1][0])

            c_out = st.container()
            with c_out:
                out_reduced = []
                for i in range(len(reduced)):
                    tmp = []
                    ranking = reduced[i][0]
                    tmp.append(ranking)
                    ss_ratio = reduced[i][1][0]
                    tmp.append(ss_ratio)
                    for j in range(len(search_attributes)):
                        if search_attributes[j] == 'location':
                            location = reduced[i][1][j]
                            tmp.append(location)
                        if search_attributes[j] == "perc intl students":
                            intl_ratio = toPercent(reduced[i][1][j])
                            tmp.append(intl_ratio)
                        if search_attributes[j] == "number students":
                            num_stu = reduced[i][1][j]
                            tmp.append(num_stu)
                    out_reduced.append(tmp)

                column_name = ['Ranking'] + search_attributes
                output_dataframe = pd.DataFrame(out_reduced, columns=column_name)
                st.dataframe(output_dataframe)
    return


def main():
    st.title('Search in Firebase')

    c_URL = st.container()
    databaseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'
    # databaseURL = c_URL.text_input('Please enter your database URL:  ')
    # if databaseURL:
    #     c_URL.success('✔️ Success!')
    # else:
    #     st.stop()

    ############################################# TEST

    multiselect_2 = ['location', "perc intl students", "number students"]
    search_attributes = ['students staff ratio'] + multiselect_2
    map3 = mapPartition(databaseURL, "universities_ranking_P", "1", key="ranking", value=search_attributes)

    reduced = map3
    red_loc = reducer(reduced, 'option_2', ["location", 'Australia'], multiselect=multiselect_2)
    reduced = red_loc
    red_intl = reducer(reduced, 'option_2', ["perc intl students", "above", 0.2], multiselect=multiselect_2)
    reduced = red_intl
    red_numStu = reducer(reduced, 'option_2', ["number students", "above", 10000], multiselect=multiselect_2)
    reduced = red_numStu
    reduced.sort(key=lambda x: x[1][0])


    out_reduced = []
    for i in range(len(reduced)):
        tmp = []
        ranking = reduced[i][0]
        tmp.append(ranking)
        ss_ratio = reduced[i][1][0]
        tmp.append(ss_ratio)
        for j in range(len(search_attributes)):
            if search_attributes[j] == 'location':
                location = reduced[i][1][j]
                tmp.append(location)
            if search_attributes[j] == "perc intl students":
                intl_ratio = toPercent(reduced[i][1][j])
                tmp.append(intl_ratio)
            if search_attributes[j] == "number students":
                num_stu = reduced[i][1][j]
                tmp.append(num_stu)
        out_reduced.append(tmp)

    column_name = ['Ranking'] + search_attributes
    output_dataframe = pd.DataFrame(out_reduced, columns=column_name)
    st.dataframe(output_dataframe)
    st.dataframe(output_dataframe.head(5))
    #############################################

    c_options = st.container()
    with c_options:
        options_list = [" ", "Find universities ranking within certain range...",
                        "Find TOP universities with students staff ratio..."]
        option_selectbox = st.selectbox("Please choose one searching options down below", options_list
                                        )
        if option_selectbox == options_list[0]:
            st.stop()
        if option_selectbox == options_list[1]:
            search_ranking(databaseURL)
        if option_selectbox == options_list[2]:
            search_students_staff_ratio(databaseURL)


if __name__ == "__main__":
    main()
