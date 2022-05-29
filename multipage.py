"""This module defines the multipage object used by streamlit frontend"""
from typing import Any, Callable
import streamlit as st


# Define the multipage class to manage the multiple apps in our program
class Multipage:
    """
    Framework for combining multiple streamlit apps
    """

    def __init__(self) -> None:
        self.pages = dict()

    def add_page(self, title: str, func: Callable,
                 args: tuple[Any, ...] | None = None,
                 kwargs: dict[Any] | None = None) -> None:
        """
        This module adds the page to the main app.

        Args:
            title (str): Title of the page
            func (Callable): Function that renders the app
            args: Arguments that are supplied to the function func
            kwargs: Keyword arguments supplied to the function func
        """
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()

        self.pages[title] = {
            'function': func,
            'args': args,
            'kwargs': kwargs,
        }

    def run(self):
        """This method handles page navigation"""

        current_page = st.sidebar.selectbox(
            'Go to page:',
            self.pages.keys(),
        )
        current_app = self.pages[current_page]
        current_app['function'](*current_app['args'], **current_app['kwargs'])
