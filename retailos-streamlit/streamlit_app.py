import streamlit as st
import requests

st.title("RetailOS Dashboard")

response = requests.get("https://retailos-main-dcfa576.kuberns.cloud/")
st.json(response.json())
