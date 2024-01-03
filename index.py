import streamlit as st
from mq.mqclient import TaskProducer

producer = TaskProducer()

prompt = st.text_input('Prompt', "")

st.title("Data")
data = {"prompt" : prompt}
st.json(data)

if st.button("Submit"):
    producer.send(data)