from sre_constants import SUCCESS
import streamlit as st
import pandas as pd


def sucess():
    st.markdown("# SUCCESS")


def page2():
    st.markdown("# Page 2 ❄️")
    st.sidebar.markdown("# Page 2 ❄️")


def page3():
    st.markdown("# Page 3 🎉")
    st.sidebar.markdown("# Page 3 🎉")


# page_names_to_funcs = {
#    "Main Page": main_page,
#    "Page 2": page2,
#    "Page 3": page3,
# }

def main():
    sucess()

    # selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())
    # page_names_to_funcs[selected_page]()


if __name__ == "__main__":
    main()
