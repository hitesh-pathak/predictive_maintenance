"""
This module  renders the prediction page of streamlit app
"""
import streamlit as st
from utils import manager

from predict_from_model import Prediction
from prediction_validation_insertion import PredictionValidation

# load other sections here
from pages import download


def app(queue, connection):
    st.markdown('## Predict RUL')

    form = st.form(key='predict_form')

    methods = ['Predict from stock files',
               ]

    check_method = form.selectbox('Choose method: ',
                                  options=methods,
                                  )

    check_download = form.checkbox('Download Prediction files')

    submitted = form.form_submit_button('Start Predicting')

    if submitted:
        if check_method == methods[0]:

            st.info('Importing files from database')
            importer = PredictionValidation()
            # this is a deterministic function so cache it

            manager(queue, connection,
                    func=importer.pred_fetch, args=None, timeout=900, max_wait=1000,
                    failure_msg='Importing files from database failed',
                    success_msg='Imported files from database successfully!')

            st.info('Generating predictions from imported data')
            predictor = Prediction('Prediction_FileFromDB')
            # this depends upon the files, so let's not cache it
            manager(queue, connection,
                    func=predictor.prediction_from_model, args=None, timeout=300, max_wait=400,
                    failure_msg='Failed to generate predictions.',
                    success_msg='Predictions saved successfully!')

        if check_download:
            download.app(path='Prediction_output',
                         button_text='Download Prediction files',
                         output_name='prediction_output')
