from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class Raw_Data_validation:
    """
             This class shall be used for handling all the validation done on the Raw Training Data.

    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()

        # preliminary checks
        if not os.path.isfile(self.schema_path):
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, f"Error: Schema file {self.schema_path} does not exist. Aborting!")
            file.close()
            raise FileNotFoundError(f"Schema file {self.schema_path} does not exist. Aborting!")

    def valuesFromSchema(self):
        """
                        Method Name: values_from_schema
                        Description: This method extracts all the relevant information
                        from the pre-defined "Schema" file.
                        Output: column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception
        """
        try:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, f'Loading schema from {self.schema_path}.')
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            column_names = dic['ColName']
            NumberofColumns = dic['number_of_columns']

            message = "Training file schema values loaded." + \
                      f"\tsample file name: {pattern}\tNumber of columns: {NumberofColumns}"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        else:
            return column_names, NumberofColumns

    @staticmethod
    def manualRegexCreation():
        """
                                Method Name: manual_regex_creation
                                Description: This method contains a manually defined regex based
                                on the "FileName" given in "Schema" file.This Regex is used to validate
                                 the filename of the training data.
                                Output: Regex pattern
                                On Failure: None
        """
        pattern = r"^train_FD00[1-4]\.txt$|^train_00[1-4]\.csv$"
        # pre compiled regex for optimisation
        regex = re.compile(pattern)

        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: create_directory_for_good_bad_raw_data
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

        """
        try:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Making good and bad raw data directories.")
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            file.close()
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % e)
            file.close()
            raise OSError
        else:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Good and Bad raw data directories created successfully.")

    def deleteExistingGoodDataTrainingFolder(self):
        """
                                    Method Name: deleteExistingGoodDataTrainingFolder
                                    Description: This method deletes the directory made  to store the Good Data
                                                after loading the data in the table. Once the good files are
                                                loaded in the DB,deleting the directory ensures space optimization.
                                    Output: None
                                    On Failure: OSError

        """
        try:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Removing existing Good data training folder")
            path = 'Training_Raw_files_validated/'

            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')

                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
            file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
                                    Method Name: deleteExistingBadDataTrainingFolder
                                    Description: This method deletes the directory made to store the bad Data.
                                    Output: None
                                    On Failure: OSError

        """

        try:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Removing existing bad raw data directory.")
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')

                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

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
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Copying bad raw data files to archive directory")
            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date) + "_" + str(time)

                if not os.path.isdir(dest):
                    os.makedirs(dest)
                self.logger.log(file, f"Archive folder created : {dest}")

                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)

                self.logger.log(file, "Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                self.logger.log(file, "Bad files archived. Now removing bad data directory.")
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")

                file.close()

            else:
                self.logger.log(file, "Bad raw data directory does not exist. Can not archive bad data.")
                file.close()
                raise NotADirectoryError("Bad raw data directory does not exist. Can not archive bad data.")

        except OSError as ose:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error :{ose}")
            file.close()
            raise ose

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validationFileNameRaw(self, regex):
        """
                    Method Name: validation_filename_raw
                    Description: This function validates the name of the training csv files
                                    as per given name in the schema! Regex pattern is used to do the validation.
                                    If name format do not match the file is moved to Bad Raw Data folder
                                    else in Good raw data.
                    Output: None
                    On Failure: Exception
        """

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        f = open("Training_Logs/nameValidationLog.txt", 'a+')
        self.logger.log(f, "Starting validation of file name.")
        self.logger.log(f, "Deleting existing good and bad raw data directories.")
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        # create new directories
        self.logger.log(f, "Creating new good and bad raw data directories.")
        self.createDirectoryForGoodBadRawData()
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
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Matching file  names with regex.")
            for filename in onlyfiles:
                filepath = os.path.join(self.Batch_Directory, filename)
                if regex.match(filename):
                    shutil.copy(filepath, "Training_Raw_files_validated/Good_Raw")
                    self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                else:
                    shutil.copy(filepath, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            self.logger.log(f, "Name validation finished.")
            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while validating FileName %s" % e)
            f.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
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
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file, sep=r'\s+', header=None)
                if csv.shape[1] == NumberofColumns:
                    self.logger.log(f, "Column Length for the file validated :: %s" % file)
                else:
                    shutil.move(
                        "Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")

        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occurred:: %s" % e)
            f.close()
            raise e
        else:
            f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                            Method Name: validate_missing_values_in_whole_column
                            Description: This function validates if any column in the csv file has all values missing.
                                        If all the values are missing, the file is not suitable for processing.
                                        Such files are moved to bad raw data.
                            Output: None
                            On Failure: Exception
        """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file, sep=r'\s+', header=None)
                count = 0
                for column in csv:
                    # if (len(csv[column]) - csv[column].count()) == len(csv[column]):
                    if csv[column].count() == 0:
                        count += 1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,
                                        "A column contains all NULL values!! File moved to Bad Raw Folder :: %s" % file)
                        break

                if count == 0:
                    # csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
                    self.logger.log(f, "No completely null columns found in file :: %s" % file)

        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred:: %s" % e)
            f.close()
            raise e
        else:
            f.close()
