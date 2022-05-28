'''
This module takes care of data ingestion.
'''
import json
import os
import re

import pandas as pd


class DataGetter:
    """
    This class shall  be used for obtaining the data files for training.
    """
    def __init__(self, file_object, logger_object,
                mode: str='train', path: str='Training_FileFromDB'):

        self.file_object = file_object
        self.logger_object = logger_object
        self.mode = mode

        if self.mode not in ['predict', 'test', 'train']:
            error = Exception("mode must be either 'predict', 'test' or 'train'.")
            self.logger_object(self.file_object, f"Error: {error}!")
            raise error

        self.loading_directory = path

        # prelim checks
        if not os.path.isdir(self.loading_directory):
            error = NotADirectoryError('Loading directory does not exist, exiting.')
            self.logger_object.log(self.file_object, f'Error: {error}')
            raise error
        if not [f for f in os.listdir(self.loading_directory)
                  if os.path.isfile(os.path.join(self.loading_directory, f))]:
            error = FileNotFoundError('Loading directory does not contain any files, exiting.')
            self.logger_object.log(self.file_object, f"Error: {error}")
            raise error

        # load column headers for later use
        if not os.path.isfile('schema_prediction.json'):
            error = FileNotFoundError('Required schema file not found.')
            self.logger_object.log(self.file_object, f"{error}")
            raise error

        with open('schema_prediction.json', 'r', encoding='utf-8') as schema:
            dic = json.load(schema)
            column_names = dic['ColName']

        self.column_names = column_names

    def get_data(self):
        """
        Method Name: get_data

        Description: This method reads the training input files and outputs pandas dataframes.

        Output: A pandas DataFrame.

        On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object,
                                'Entered the get_data method of the Data_Getter class')
        try:

            if self.mode == 'train':
                regex = re.compile(r"^train_input_00[1-4]\.csv$")
            else:
                regex = re.compile(r"^test_input_00[1-4]\.csv$")

            self.logger_object.log(self.file_object,
                                    f"Checking all valid files in {self.loading_directory}.")

            onlyfiles = [f for f in os.listdir(self.loading_directory)
                         if os.path.isfile(os.path.join(self.loading_directory, f))
                          and regex.match(f)]

            if not onlyfiles:
                error = FileNotFoundError(
                            f"No valid files found in {self.loading_directory}. Quitting!")
                self.logger_object.log(self.file_object, f"Error: {error}")
                raise error
            self.logger_object.log(self.file_object,
                                    "Generating dataframe generator object for files.")

            # define data generator object
            def data_gen():
                for file in onlyfiles:
                    file_path = os.path.join(self.loading_directory, file)
                    df = pd.read_csv(file_path, header=None, names=self.column_names.keys())
                    yield df, file

            self.logger_object.log(self.file_object,
                                   'Data Load Successful' + \
                                       f'Exited the get_data method of the {__class__}')
            return data_gen

        except Exception as e:
            self.logger_object.log(self.file_object,
                                f'Error in get_data method of the Data_Getter class: {e}')
            self.logger_object.log(self.file_object,
                            f'Data Load Unsuccessful.Exited the get_data method of {__class__}')
            raise e
