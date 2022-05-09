from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from application_logging import logger
import os


class train_validation:
    def __init__(self, path):
        # first create the log directory before anything
        if not os.path.isdir('Training_Logs'):
            os.makedirs('Training_Logs')
        self.log_path = "Training_Logs/Training_Main_Log.txt"
        self.file_object = open(self.log_path, 'a+')
        self.log_writer = logger.App_Logger()
        self.path = path
        # self.raw_data = Raw_Data_validation(path)
        # self.dBOperation = dBOperation()

    def train_validation(self):
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
                error = FileNotFoundError("No training files found in Batch directory {self.path}.")
                self.log_writer.log(self.file_object, f"Error: {error}")
                self.file_object.close()
                raise error

            # begin validation part, create validator
            self.log_writer.log(self.file_object, 'Start validation of files.')
            validator = Raw_Data_validation(self.path)
            self.log_writer.log(self.file_object, 'Created validator object.')
            # extracting values from prediction schema
            self.log_writer.log(self.file_object, 'Getting values from schema file')
            column_names, noofcolumns = validator.valuesFromSchema()
            # getting the regex defined to validate filename
            self.log_writer.log(self.file_object, 'Getting file name regex')
            regex = Raw_Data_validation(self.path).manualRegexCreation()
            # validating filename of prediction files
            self.log_writer.log(self.file_object, 'Validating file name using regex')
            validator.validationFileNameRaw(regex)
            # validating column length in the file
            self.log_writer.log(self.file_object, 'Validating number of columns')
            validator.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.log_writer.log(self.file_object, 'Validating if any column has all NULL values.')
            validator.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting database to csv file process.")
            # create db operation instance
            db_operator = dBOperation()
            self.log_writer.log(self.file_object, "Created database operator.")
            db_operator.selectingDatafromtableintocsv('traindb')
            self.log_writer.log(self.file_object, "Exporting csv files from tables completed")

            self.log_writer.log(self.file_object, "Cleaning up raw data directories")
            validator.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted.")

            # Move the bad files to archive folder
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder.")
            validator.moveBadFilesToArchiveBad()
            validator.deleteExistingBadDataTrainingFolder()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Data validation and importing completed.")

            self.file_object.close()

        except Exception as e:
            self.log_writer.log(self.file_object, f"Error: {e}")
            self.file_object.close()
            raise e
