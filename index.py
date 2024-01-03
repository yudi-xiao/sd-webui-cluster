import streamlit as st
import asyncio
from mq.mqclient import TaskProducer, ProducerStatus

if 'producer_status' not in st.session_state:
    st.session_state.producer_status = ProducerStatus.Shutdown
    
if 'producer' not in st.session_state:
    st.session_state.producer = TaskProducer()
    
def set_producer_stat():
    st.session_state.producer_status = ProducerStatus.Running if st.session_state.producer_status == ProducerStatus.Shutdown else ProducerStatus.Shutdown
    if st.session_state.producer_status == ProducerStatus.Shutdown:
        st.session_state.producer.close()
    else:
        st.session_state.producer.open()

if st.session_state.producer_status == ProducerStatus.Running:
    st.success("Running", icon="✅")
    st.button("Close", on_click=set_producer_stat)
else:
    st.error("Shutdown", icon="🚨")
    st.button("Open", on_click=set_producer_stat)

prompt = st.text_input('Prompt', "")

st.title("Data")
data = {"prompt" : prompt}
st.json(data)

if st.button("Submit"):
    st.session_state.producer.send(data)
    
