from sre_constants import SUCCESS
import streamlit as st
import pandas as pd


def sucess():
    st.markdown("<h1 style='text-align: center;'>UNIVERSITY INFORMATION DATABASE</h1>",unsafe_allow_html=True)
    st.subheader("A database that provides insights into more than 2000 Universities worldwide to help students compare the academic quality of the highest ranked universities in each country, and the potential value of pursuing a degree at a top school.")
    st.write("\n")
    st.write("\n")
    st.write("\n")
    st.write("Team: :")
    st.write("***Jialin Tian***")
    st.write("***Yinhan Li***")
    st.write("***Benfica Fernando***")
def page2():
    st.markdown("# Page 2 ❄️")
    st.sidebar.markdown("# Page 2 ❄️")

def page3():
    st.markdown("# Page 3 🎉")
    st.sidebar.markdown("# Page 3 🎉")


def main():
   sucess()
    #selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())
    #page_names_to_funcs[selected_page]()

if __name__=="__main__":
    main()


