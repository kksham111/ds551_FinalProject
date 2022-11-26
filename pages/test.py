import streamlit as st
import requests

baseURL2 = 'https://filesystemproject-c325a-default-rtdb.firebaseio.com/'
res3 =requests.patch("https://filesystemproject-c325a-default-rtdb.firebaseio.com/Data/User/B/.json", '{"Age":20}')
print(res3,"RESULT")
st.write(res3)
