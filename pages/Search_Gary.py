import streamlit as st
import requests
import json
import pandas as pd


def search_ranking(baseURL):
    c_1, c_2 = st.columns(2)
    with c_1:
        lower_num = st.number_input("Enter lower boundary for ranking: ", min_value=1, max_value=1526)
    with c_2:
        upper_num = st.number_input("Enter upper boundary for ranking: ", min_value=1, max_value=1526)

    if lower_num and upper_num:
        if lower_num > upper_num:
            st.info("Please enter valid searching range.")
    else:
        st.stop()
    data = json.loads(requests.get(baseURL+".json").text)
    data_P1 = data['Data']['User']['universities_ranking_P1']
    data_P2 = data['Data']['User']['universities_ranking_P2']
    data_P1.sort(key=lambda k: (k.get('ranking', 0)))
    data_P2.sort(key=lambda k: (k.get('ranking', 0)))

    i,j = 0, 0
    return_list = []
    ranking_i = data_P1[i]['ranking']
    ranking_j = data_P2[j]['ranking']
    while ranking_i < lower_num:
        i += 1
        ranking_i = data_P1[i]['ranking']
    while ranking_j < lower_num:
        j += 1
        ranking_j = data_P2[j]['ranking']
    while ranking_j <= upper_num or ranking_i <= upper_num:
        if ranking_j < ranking_i:
            return_list.append(data_P2[j])
            j += 1
            ranking_j = data_P2[j]['ranking']
        else:
            return_list.append(data_P1[i])
            i += 1
            ranking_i = data_P1[i]['ranking']


    st.dataframe(return_list)



def main():
    st.title('Search in Firebase')

    c_URL = st.container()
    databaseURL = 'https://dsci551-final-project-e495b-default-rtdb.firebaseio.com/'
    c_URL_submitted = c_URL.text_input('Please enter your database URL:  ')
    if c_URL_submitted:
        c_URL.info('✔️ Succeed!')
    else:
        st.stop()

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




if __name__ == "__main__":
    main()
