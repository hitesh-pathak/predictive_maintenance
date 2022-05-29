"""This module renders the download section"""
from shutil import make_archive
import streamlit as st


def app(path: str = 'Prediction_output',
        button_text: str = 'Download',
        output_name: str = 'output', ):
    name = make_archive(output_name, 'zip', base_dir=path)

    with open(name, 'rb') as file:
        _ = st.download_button(
            label=button_text,
            data=file,
            file_name=name,
            mime='application/zip'
        )
