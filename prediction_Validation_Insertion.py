from Prediction_Raw_Data_Validation.predictionDataValidation import PredictionDataValidation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DbOperation
from application_logging import logger
import os


class PredictionValidation:  # train->pred
    def __init__(self, path):
        # first create the log directory before anything
        if not os.path.isdir('Prediction_Logs'):
            os.makedirs('Prediction_Logs')
        self.log_path = "Prediction_Logs/Prediction_Main_Log.txt"
        self.file_object = open(self.log_path, 'a+')
        self.log_writer = logger.App_Logger()
        self.path = path
        # self.raw_data = Raw_Data_validation(path)
        # self.DbOperation = DbOperation()

    def pred_validation(self):
        try:
            # init logs
            self.file_object = open(self.log_path, 'a+')
            # check batch file
            if not os.path.isdir(self.path):
                error = NotADirectoryError(f"Bath directory not found at {self.path}")
                self.log_writer.log(self.file_object, f"Error: {error}")
                self.file_object.close()
                raise error
            elif not os.listdir(self.path):
                error = FileNotFoundError("No prediction files found in Batch directory {self.path}.")
                self.log_writer.log(self.file_object, f"Error: {error}")
                self.file_object.close()
                raise error

            # begin validation part, create validator
            self.log_writer.log(self.file_object, 'Start validation of files.')
            validator = PredictionDataValidation(self.path)
            self.log_writer.log(self.file_object, 'Created validator object.')
            # extracting values from prediction schema
            self.log_writer.log(self.file_object, 'Getting values from schema file')
            column_names, no_of_columns = validator.values_from_schema()
            # getting the regex defined to validate filename
            self.log_writer.log(self.file_object, 'Getting file name regex')
            regex = validator.manual_regex_creation()
            # validating filename of prediction files
            self.log_writer.log(self.file_object, 'Validating file name using regex')
            validator.validation_filename_raw(regex)
            # validating column length in the file
            self.log_writer.log(self.file_object, 'Validating number of columns')
            validator.validate_column_length(no_of_columns)
            # validating if any column has all values missing
            self.log_writer.log(self.file_object, 'Validating if any column has all NULL values.')
            validator.validate_missing_values_in_whole_column()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting database to csv file process.")
            # create db operation instance
            db_operator = DbOperation()
            self.log_writer.log(self.file_object, "Created database operator.")
            db_operator.selecting_data_from_table_into_csv('predictdb')  # traindb->predictdb
            self.log_writer.log(self.file_object, "Exporting csv files from tables completed")

            self.log_writer.log(self.file_object, "Cleaning up raw data directories")
            validator.delete_existing_good_data_prediction_folder()  # Training->Prediction
            self.log_writer.log(self.file_object, "Good_Data folder deleted.")

            # Move the bad files to archive folder
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder.")
            validator.move_bad_files_to_archive_bad()
            validator.delete_existing_bad_data_prediction_folder()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Data validation and importing completed.")

            self.file_object.close()

        except Exception as e:
            self.log_writer.log(self.file_object, f"Error: {e}")
            self.file_object.close()
            raise e
