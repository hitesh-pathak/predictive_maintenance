import pandas as pd
import os
import re


class Data_Getter:
    """
    This class shall  be used for obtaining the data files for training.
    """
    def __init__(self, file_object, logger_object):
        self.training_directory = 'Training_FileFromDB/'
        self.file_object = file_object
        self.logger_object = logger_object

        # prelim checks
        if not os.path.isdir(self.training_directory):
            error = NotADirectoryError('Training directory does not exist, exiting.')
            self.logger_object.log(self.file_object, f'Error: {error}')
            raise error
        elif not [f for f in os.listdir(self.training_directory)
                  if os.path.isfile(os.path.join(self.training_directory, f))]:
            error = FileNotFoundError('Training directory does not contain any files, exiting.')
            self.logger_object.log(self.file_object, f"Error: {error}")
            raise error

    def get_data(self):
        """
        Method Name: get_data

        Description: This method reads the training input files and outputs pandas dataframes.

        Output: A pandas DataFrame.

        On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the get_data method of the Data_Getter class')
        try:
            # file_list = ['train_input_00' + k + '.csv' for k in range(1, 5)]
            regex = re.compile(r"^train_input_00[1-4]\.csv$")
            self.logger_object.log(self.file_object, f"Checking all valid files in {self.training_directory}.")
            onlyfiles = [f for f in os.listdir(self.training_directory)
                         if os.path.isfile(os.path.join(self.training_directory, f)) and regex.match(f)]

            if not onlyfiles:
                error = FileNotFoundError(f"No valid files found in {self.training_directory}. Quitting!")
                self.logger_object.log(self.file_object, f"Error: {error}")
                raise error
            self.logger_object.log(self.file_object, "Generating dataframe generator object for files.")

            # define data generator object
            def data_gen():
                for f in onlyfiles:
                    file_path = os.path.join(self.training_directory, f)
                    df = pd.read_csv(file_path)
                    yield df, f

            self.logger_object.log(self.file_object,
                                   'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return data_gen

        except Exception as e:
            self.logger_object.log(self.file_object, f'Error in get_data method of the Data_Getter class: {e}')
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise e
