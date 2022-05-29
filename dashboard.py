"""
Streamlit dashboard
"""
import streamlit as st

from rq import Queue
from worker import conn

from multipage import Multipage
from pages import predict

# page set up
st.set_page_config(
     page_title="RUL predictor 3000",
     layout="wide",
 )

# create needed objects
q = Queue(connection=conn)
app = Multipage()


st.title('RUL predictor 3000')


# add needed sections in the app
app.add_page('Predict RUL', predict.app, kwargs={'queue': q, 'connection': conn})
