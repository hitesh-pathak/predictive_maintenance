'''
This module handles validation, insertion into database, importing from database operations
for prediction files.
'''
import os

from application_logging import logger
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import \
    DbOperation
from Prediction_Raw_Data_Validation.predictionDataValidation import \
    PredictionDataValidation


class PredictionValidation:
    """
    This class validates the prediction files in the given directory.
    """

    def __init__(self):
        # first create the log directory before anything
        if not os.path.isdir('Prediction_Logs'):
            os.makedirs('Prediction_Logs')
        self.log_path = "Prediction_Logs/Prediction_Main_Log.txt"
        self.log_writer = logger.App_Logger()

    def pred_validation(self, path: str):
        """
        This method validates the files in path. According sorts the files in different directories.

        """
        with open(self.log_path, 'a+', encoding='utf-8') as file_object:
            try:
                # check batch file
                if not os.path.isdir(path):
                    error = NotADirectoryError(f"Bath directory not found at {path}")
                    self.log_writer.log(file_object, f"Error: {error}")
                    raise error
                if not os.listdir(path):
                    error = FileNotFoundError(
                                "No prediction files found in Batch directory {path}.")
                    self.log_writer.log(file_object, f"Error: {error}")
                    raise error

                self.log_writer.log(file_object, "Cleaning up old raw data directories")
                validator = PredictionDataValidation(path)
                validator.delete_existing_good_data_prediction_folder()
                self.log_writer.log(file_object, "Good_Data folder deleted.")

                # Move the bad files to archive folder
                self.log_writer.log(file_object,
                                    "Moving bad files to Archive and deleting Bad_Data folder.")
                validator.move_bad_files_to_archive_bad()
                validator.delete_existing_bad_data_prediction_folder()
                self.log_writer.log(file_object,
                                    "Bad files moved to archive!! Bad folder Deleted!!")

                # begin validation part, create validator
                self.log_writer.log(file_object, 'Start validation of files.')

                self.log_writer.log(file_object, 'Getting values from schema file')
                _, no_of_columns = validator.values_from_schema()

                self.log_writer.log(file_object, 'Getting file name regex')
                regex = validator.manual_regex_creation()

                self.log_writer.log(file_object, 'Validating file name using regex')
                validator.validation_filename_raw(regex)

                self.log_writer.log(file_object, 'Validating number of columns')
                validator.validate_column_length(no_of_columns)

                self.log_writer.log(file_object, 'Validating if any column has all NULL values.')
                validator.validate_missing_values_in_whole_column()

                self.log_writer.log(file_object, "Raw Data Validation Complete!!")

            except Exception as e:
                self.log_writer.log(file_object, f"Error: {e}")
                raise


    def pred_insertion(self):
        """
        This method inserts the validated data files into database tables.

        """

        with open(self.log_path, 'a+', encoding='utf-8') as file_object:
            try:

                self.log_writer.log(file_object, "Starting database insertion.")
                # create db operation instance
                db_operator = DbOperation()
                self.log_writer.log(file_object, "Created database operator.")
                db_operator.insert_into_table_good_data('predictdb')

                self.log_writer.log(file_object, "Data insertion completed")

            except Exception as e:
                self.log_writer.log(file_object, f"Error: {e}")
                raise


    def pred_fetch(self):
        """
            This method fetches the prediction data set already stored in the database.
        """

        with open(self.log_path, 'a+', encoding='utf-8') as file_object:
            try:
                self.log_writer.log(file_object, "Fetching data sets from database.")
                db_operator = DbOperation()
                self.log_writer.log(file_object, "Created database operator.")

                db_operator.selecting_data_from_table_into_csv('predictdb', flush=False)
                self.log_writer.log(file_object, "Data imported from database.")

            except Exception as e:
                self.log_writer.log(file_object, f"Error: {e}")
                raise
