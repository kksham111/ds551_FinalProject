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
    if option == 'option_0':

        for item in input_map:
            lst = []
            u_ranking = item[0]
            u_name = item[1]
            lst.append(u_ranking)
            lst.append(u_name)
            for i in range(len(queries)):
                lst.append(item[2][i])
            return_lst.append(lst)


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
                index_loc = multiselect.index("location")
                if item[2][index_loc] == queries[1]:
                    return_lst.append(item)
            if quer_key == "perc intl students":
                index_intl = multiselect.index("perc intl students")
                intl_perc = queries[2]
                if queries[1] == "above":
                    if item[2][index_intl] > intl_perc:
                        return_lst.append(item)
                else:
                    if item[2][index_intl] < intl_perc:
                        return_lst.append(item)
            if quer_key == "number students":
                index_numstu = multiselect.index("number students")
                num_edge_stu = queries[2]
                if queries[1] == "above":
                    if item[2][index_numstu] > num_edge_stu:
                        return_lst.append(item)
                else:
                    if item[2][index_numstu] < num_edge_stu:
                        return_lst.append(item)

    return return_lst


def list_combiner(list1, list2, list3):
    return list1 + list2 + list3


def search_scores(databaseURL):

    multiselect_1 = st.multiselect("Additional Attributes options",
                                   options=("overall score","teaching score","research score","citations score",
                                            "industry income score","intl outlook score"))
    if multiselect_1:
        button_o0 = st.button("Continue", key="continue0")
        if button_o0:
            map1 = mapPartition(databaseURL, "universities_scores", "1", key="ranking", value=multiselect_1)
            map2 = mapPartition(databaseURL, "universities_scores", "2", key="ranking", value=multiselect_1)
            map3 = mapPartition(databaseURL, "universities_scores", "3", key="ranking", value=multiselect_1)
            map_all = list_combiner(map1, map2, map3)
            satisfied = reducer(map_all, 'option_0', multiselect_1, None)
            satisfied.sort(key=lambda x: x[0])
            column_title = ['Ranking', 'University Title'] + multiselect_1
            output_dataframe = pd.DataFrame(satisfied, columns=column_title)
            st.dataframe(output_dataframe)


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

    # print result for map
    st.write("First some lines of the result MAP1 comes from the map function:")
    st.write(map1[0:5])

    map2 = mapPartition(databaseURL, "universities_ranking_P", "2", key="ranking", value=['title'])
    map3 = mapPartition(databaseURL, "universities_ranking_P", "3", key="ranking", value=['title'])
    map_all = list_combiner(map1, map2, map3)

    satisfied = reducer(map_all, 'option_1', ['ranking', lower_num, upper_num], None)

    # print result after reduced
    st.write("First some lines of the result REDUCED after the reduce function:")
    st.write(satisfied[0:5])

    satisfied.sort(key=lambda x: x[0])

    c_out = st.container()
    with c_out:
        output_dataframe = pd.DataFrame(satisfied, columns=['Ranking', 'UniversityName'])
        st.dataframe(output_dataframe)


def search_students_staff_ratio(databaseURL):

    num = st.slider("Select top xxx universities to return: ", key="o2num", min_value=0, max_value=20)
    multiselect_2 = st.multiselect("Additional Attributes options",
                                   options=("location", "perc intl students", "number students"))
    if multiselect_2:
        f"Attributes selected: {'; '.join(i for i in multiselect_2)}."

        if "location" in multiselect_2:
            country = st.text_input(label="Please input a country name:", key='location')
            if country:
                st.write("Results contain universities in", country, "only.")
        if "perc intl students" in multiselect_2:
            c_2_l, c_2_r = st.columns(2)
            with c_2_l:
                my_radio = st.radio(label=" ", key="intl_ratio_radio", options=("above", "below"), label_visibility='hidden')
            with c_2_r:
                ratio_2 = st.number_input("Please enter a number -- internatial student ratio", key="ratio2", min_value=0, max_value=100, value=0)
            if my_radio and ratio_2:
                st.write("Results contain universities that have a international student ratio ", my_radio, ratio_2,
                         "%")
        if "number students" in multiselect_2:
            c_2_l2, c_2_r2 = st.columns(2)
            with c_2_l2:
                my_radio2 = st.radio(label=" ", key="numstu_radio", options=("above", "below"),
                                     label_visibility='hidden')
            with c_2_r2:
                ratio_22 = st.number_input("Please enter a number -- number of total students", key="numstu_num", min_value=0, value=10000)
            if my_radio2 and ratio_22:
                st.write("Results contain universities that have students number ", my_radio2, ratio_22)

    # st.write(st.session_state)
    button_o2_2 = st.button("Continue", key="continue2")

    if button_o2_2:
        country, my_radio, ratio_2_perc, my_radio2, ratio_22,num = 0,0,0,0,0,0
        if "location" in st.session_state:
            country = st.session_state['location']
        if "intl_ratio_radio" in st.session_state:
            my_radio = st.session_state["intl_ratio_radio"]
        if "ratio2" in st.session_state:
            ratio_2 = st.session_state['ratio2']
            ratio_2_perc = ratio_2 / 100
        if "numstu_radio" in st.session_state:
            my_radio2 = st.session_state["numstu_radio"]
        if "numstu_num" in st.session_state:
            ratio_22 = st.session_state["numstu_num"]
        if "o2num" in st.session_state:
            num = st.session_state["o2num"]
        # st.write(country, my_radio, ratio_2_perc, my_radio2, ratio_22)

        search_attributes = ['students staff ratio'] + multiselect_2
        map1 = mapPartition(databaseURL, "universities_ranking_P", "1", key="ranking", value=search_attributes)

        # print result for map
        st.write("First some lines of the result MAP1 comes from the map function:")
        st.write(map1[0:5])

        map2 = mapPartition(databaseURL, "universities_ranking_P", "2", key="ranking", value=search_attributes)
        map3 = mapPartition(databaseURL, "universities_ranking_P", "3", key="ranking", value=search_attributes)
        map_all = list_combiner(map1, map2, map3)

        # reduce with additional attributes first
        reduced = map_all
        if "location" in multiselect_2:
            red_loc = reducer(reduced, 'option_2', ["location", country], search_attributes)
            reduced = red_loc
        if "perc intl students" in multiselect_2:
            red_intl = reducer(reduced, 'option_2', ["perc intl students", my_radio, ratio_2_perc], search_attributes)
            reduced = red_intl
        if "number students" in multiselect_2:
            red_numStu = reducer(reduced, 'option_2', ["number students", my_radio2, ratio_22], search_attributes)
            reduced = red_numStu

        # print result after reduced
        st.write("First some lines of the result REDUCED after the reduce function:")
        st.write(reduced[0:5])

        # then find the top xx universities with staff ratio
        reduced.sort(key=lambda x: x[2][0])
        # st.dataframe(reduced)
        out_reduced = []
        for i in range(len(reduced)):
            tmp = []
            ranking = reduced[i][0]
            tmp.append(ranking)
            title = reduced[i][1]
            tmp.append(title)
            ss_ratio = format(reduced[i][2][0], '.1f')
            tmp.append(ss_ratio)
            for j in range(len(search_attributes)):
                if search_attributes[j] == 'location':
                    location = reduced[i][2][j]
                    tmp.append(location)
                if search_attributes[j] == "perc intl students":
                    intl_ratio = toPercent(reduced[i][2][j])
                    tmp.append(intl_ratio)
                if search_attributes[j] == "number students":
                    num_stu = reduced[i][2][j]
                    tmp.append(num_stu)
            out_reduced.append(tmp)

        column_name = ['Ranking', 'Title'] + search_attributes
        output_dataframe = pd.DataFrame(out_reduced, columns=column_name).head(num)
        st.write(output_dataframe)
    return


def main():
    st.title('Search in Firebase')

    c_URL = st.container()
    st.text("Sample URL: https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/")
    # databaseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'
    databaseURL = c_URL.text_input('Please enter your database URL:  ')
    if databaseURL:
        c_URL.success('✔️ Success!')
    else:
        st.stop()

    ############################################# TEST
    multiselect_1 = ["overall score","teaching score","research score","citations score",
                                            "industry income score","intl outlook score"]
    map1 = mapPartition(databaseURL, "universities_scores", "1", key="ranking", value=multiselect_1)
    map2 = mapPartition(databaseURL, "universities_scores", "2", key="ranking", value=multiselect_1)
    map3 = mapPartition(databaseURL, "universities_scores", "3", key="ranking", value=multiselect_1)
    map_all = list_combiner(map1, map2, map3)
    satisfied = reducer(map_all, 'option_0', multiselect_1, None)
    #############################################

    c_options = st.container()
    with c_options:
        options_list = [" ", "Find ...(score) from the dataset ",
                        "Find universities ranking within certain range...",
                        "Find TOP universities with students staff ratio..."]
        option_selectbox = st.selectbox("Please choose one searching options down below", options_list
                                        )
        if option_selectbox == options_list[0]:
            st.stop()
        if option_selectbox == options_list[1]:
            search_scores(databaseURL)
        if option_selectbox == options_list[2]:
            search_ranking(databaseURL)
        if option_selectbox == options_list[3]:
            search_students_staff_ratio(databaseURL)



if __name__ == "__main__":
    main()
