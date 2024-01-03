import streamlit as st
import asyncio

st.set_page_config(layout="wide")

if "count" not in st.session_state:
    st.session_state.count = 0

if "enable" not in st.session_state:
    st.session_state.enable = False

async def watch(t: st._DeltaGenerator):
    while True:
        # use session_state for data cache
        if not st.session_state.enable:
            continue
        await asyncio.sleep(1)
        st.session_state.count += 1
        st.rerun()

t = "On" if st.session_state.enable else "Off"

st.text(f"{t} {st.session_state.count}")

st.toggle("Enable", key="enable")
        
test = st.empty()

asyncio.run(watch(test))