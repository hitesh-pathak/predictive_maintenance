from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class PredictionDataValidation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data.

    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()

        # preliminary checks
        if not os.path.isfile(self.schema_path):
            file = open("Prediction_Logs/values_from_schema_validation_log.txt", 'a+')
            self.logger.log(file, f"Error: Schema file {self.schema_path} does not exist. Aborting!")
            file.close()
            raise FileNotFoundError(f"Schema file {self.schema_path} does not exist. Aborting!")

    def values_from_schema(self):
        """
                    Method Name: values_from_schema

                    Description: This method extracts all the relevant information from the pre-defined "Schema" file.

                    Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns

                    On Failure: Raise ValueError,KeyError,Exception

        """
        try:
            file = open("Prediction_Logs/values_from_schema_validation_log.txt", 'a+')
            self.logger.log(file, f'Loading schema from {self.schema_path}.')
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            column_names = dic['ColName']
            number_of_columns = dic['NumberofColumns']

            message = "Prediction file schema values loaded." + \
                      f"\tsample file name: {pattern}\tNumber of columns: {number_of_columns}"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Prediction_Logs/values_from_schema_validation_log.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_prediction.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/values_from_schema_validation_log.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/values_from_schema_validation_log.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        else:
            return column_names, number_of_columns

    @staticmethod
    def manual_regex_creation():
        """
                      Method Name: manual_regex_creation
                      
                      Description: This method contains a manually defined regex based on the
                                    "FileName" given in "Schema" file.
                                  This Regex is used to validate the filename of the prediction data.

                      Output: Regex pattern

                      On Failure: None

        """
        pattern = r"^test_FD00[1-4]\.txt$|^train_00[1-4]\.csv$"
        # pre compiled regex for optimisation
        regex = re.compile(pattern)

        return regex

    def create_directory_for_good_bad_raw_data(self):

        """
                                        Method Name: create_directory_for_good_bad_raw_data

                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None

                                        On Failure: OSError

    """
        try:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Making good and bad raw data directories.")
            path = os.path.join("Prediction_Raw_Files_Validated", "Good_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated", "Bad_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
            file.close()
        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while creating Directory {ex}")
            file.close()
            raise OSError
        else:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Good and Bad raw data directories created successfully.")

    def delete_existing_good_data_prediction_folder(self):
        """
                                Method Name: delete_existing_good_data_prediction_folder

                                Description: This method deletes the directory made to store the Good Data
                                              after loading the data in the table. Once the good files are
                                              loaded in the DB,deleting the directory ensures space optimization.

                                Output: None

                                On Failure: OSError

        """
        try:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Removing existing Good data folder")
            path = 'Prediction_Raw_Files_Validated'

            if os.path.isdir(os.path.join(path, 'Good_Raw')):
                shutil.rmtree(os.path.join(path, 'Good_Raw'))

                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
            file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while Deleting Directory : {s}")
            file.close()
            raise OSError

    def delete_existing_bad_data_prediction_folder(self):

        """
                                Method Name: delete_existing_bad_data_prediction_folder

                                Description: This method deletes the directory made to store the bad Data.

                                Output: None

                                On Failure: OSError

        """

        try:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Removing existing bad raw data directory.")
            path = 'Prediction_Raw_Files_Validated'
            if os.path.isdir(os.path.join(path, 'Bad_Raw')):
                shutil.rmtree(os.path.join(path, 'Bad_Raw'))

                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while Deleting Directory : {s}")
            file.close()
            raise OSError

    def move_bad_files_to_archive_bad(self):

        """
                                Method Name: move_bad_files_to_archive_bad

                                Description: This method deletes the directory made  to store the Bad Data
                                              after moving the data in an archive folder. We archive the bad
                                              files to send them back to the client for invalid data issue.

                                Output: None

                                On Failure: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Copying bad raw data files to archive directory")
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "PredictionArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = os.path.join(path, 'BadData_' + str(date) + "_" + str(time))
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                self.logger.log(file, f"Archive folder created : {dest}")

                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(os.path.join(source, f), dest)

                self.logger.log(file, "Bad files moved to archive")
                path = 'Prediction_Raw_Files_Validated'
                self.logger.log(file, "Bad files archived. Now removing bad data directory.")
                if os.path.isdir(os.path.join(path, 'Bad_Raw')):
                    shutil.rmtree(os.path.join(path, 'Bad_Raw'))
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")

                file.close()

            else:
                self.logger.log(file, "Bad raw data directory does not exist. Can not archive bad data.")
                file.close()
                raise NotADirectoryError("Bad raw data directory does not exist. Can not archive bad data.")

        except OSError as ose:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error :{ose}")
            file.close()
            raise ose

        except Exception as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while moving bad files to archive:: {e}")
            file.close()
            raise e

    def validation_filename_raw(self, regex):
        """
            Method Name: validation_filename_raw

            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.

            Output: None

            On Failure: Exception
        """

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
        self.logger.log(f, "Starting validation of file name.")
        self.logger.log(f, "Deleting existing good and bad raw data directories.")
        self.delete_existing_bad_data_prediction_folder()
        self.delete_existing_good_data_prediction_folder()
        # create new directories
        self.logger.log(f, "Creating new good and bad raw data directories.")
        self.create_directory_for_good_bad_raw_data()
        self.logger.log(f, "Created new good and bad raw data directories.")

        onlyfiles = [file for file in listdir(self.Batch_Directory)
                     if os.path.isfile(os.path.join(self.Batch_Directory, file))]

        # abort if empty batch directory
        if not onlyfiles:
            self.logger.log(f, f"Batch directory {self.Batch_Directory} is empty. Aborting!")
            f.close()
            raise FileNotFoundError(f"Batch directory {self.Batch_Directory} is empty. Aborting!")
        f.close()

        try:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Matching file  names with regex.")
            good_path = os.path.join('Prediction_Raw_Files_Validated', 'Good_Raw')
            bad_path = os.path.join('Prediction_Raw_Files_Validated', 'Bad_Raw')
            for filename in onlyfiles:
                filepath = os.path.join(self.Batch_Directory, filename)
                if regex.match(filename):
                    shutil.copy(filepath, good_path)
                    self.logger.log(f, f"Valid File name! File moved to GoodRaw Folder :: {filename}")

                else:
                    shutil.copy(filepath, bad_path)
                    self.logger.log(f, f"Invalid File Name! File moved to Bad Raw Folder :: {filename}")
            self.logger.log(f, "Name validation finished.")
            f.close()

        except Exception as e:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while validating FileName %s" % e)
            f.close()
            raise e

    def validate_column_length(self, number_of_columns):
        """
                          Method Name: validate_column_length

                          Description: This function validates the number of columns in the csv files.
                                       It should be same as given in the schema file.
                                       If not same file is not suitable for processing
                                       and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.

                          Output: None

                          On Failure: Exception
        """
        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            good_path = os.path.join('Prediction_Raw_Files_Validated', 'Good_Raw')
            bad_path = os.path.join('Prediction_Raw_Files_Validated', 'Bad_Raw')
            for file in listdir(good_path):
                csv = pd.read_csv(os.path.join(good_path, file), sep=r'\s+', header=None)
                if csv.shape[1] == number_of_columns:
                    self.logger.log(f, f"Column Length for the file validated :: {file}")
                else:
                    shutil.move(os.path.join(good_path, file), bad_path)
                    self.logger.log(f, f"Invalid Column Length for the file!! File moved to Bad Raw Folder :: {file}")
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, f"Error Occurred while moving the file :: {OSError}")
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, f"Error Occurred:: {e}")
            f.close()
            raise e
        else:
            f.close()

    def validate_missing_values_in_whole_column(self):
        """
                  Method Name: validate_missing_values_in_whole_column

                  Description: This function validates if any column in the csv file has all values missing.
                               If all the values are missing, the file is not suitable for processing.
                               SUch files are moved to bad raw data.

                  Output: None

                  On Failure: Exception

        """
        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            good_path = os.path.join('Prediction_Raw_Files_Validated', 'Good_Raw')
            bad_path = os.path.join('Prediction_Raw_Files_Validated', 'Bad_Raw')
            for file in listdir(good_path):
                csv = pd.read_csv(os.path.join(good_path, file), sep=r'\s+', header=None)
                count = 0
                for column in csv:
                    # if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                    if csv[column].count() == 0:
                        count += 1
                        shutil.move(os.path.join(good_path, file), bad_path)
                        self.logger.log(f,
                                        f"Invalid Column Length for the file! File moved to Bad Raw Folder :: {file}")
                        break

                if count == 0:
                    # csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
                    self.logger.log(f, f"No completely null columns found in file :: {file}")

        except OSError:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, f"Error Occurred while moving the file :: {OSError}")
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, f"Error Occurred:: {e}")
            f.close()
            raise e
        else:
            f.close()
