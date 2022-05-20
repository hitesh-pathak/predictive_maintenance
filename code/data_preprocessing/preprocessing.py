import os.path
import pandas as pd
import numpy as np
import math
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler
from pandas.api.types import is_numeric_dtype
# from imblearn.over_sampling import SMOTE

class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self, data, columns):
        """
                Method Name: remove_columns

                Description: This method removes the given columns from a pandas dataframe.
                Output: A pandas DataFrame after removing the specified columns.

                On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        try:
            useful_data = data.drop(labels=columns, axis=1) # drop the labels specified in the columns
            self.logger_object.log(self.file_object,
                                   'Column removal Successful. Exiting remove_columns method.')
            return useful_data
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in removing columns: {e}')
            self.logger_object.log(self.file_object, 'Exiting the remove_columns method of the Preprocessor class.')
            raise e

    def separate_label_feature(self, data, label_column_name):
        """
                Method Name: separate_label_feature

                Description: This method separates the features and a Label Columns.
                Output: Returns two separate Dataframes, one containing features and the other containing Labels.

                On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def is_null_present(self,data, filename):
        """
            Method Name: is_null_present

            Description: This method checks whether there are null values present in the pandas Dataframe or not.
            Output: Returns a Boolean Value. True if null values are present in the DataFrame,
            False if they are not present.

            On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        try:
            null_present = False
            null_counts = data.isna().sum()  # check for the count of null values per column
            for col in null_counts.index:
                if null_counts[col] > 0:
                    null_present=True
                    self.logger_object.log(self.file_object, f"Null values present in {col} column.")
                    break

            if null_present:
                self.logger_object.log(self.file_object, "Writing null value count to file.")
                self.logger_object.log(self.file_object, "Creating preprocessing data directory if not present.")

                # create different directory for each datafile, based on the filename
                save_path = os.path.join('preprocessing_data/', filename)
                if not os.path.isdir(save_path):
                    os.makedirs(save_path)

                pd.DataFrame(null_counts,
                             columns=['Missing value count']).to_csv(os.path.join(save_path, 'null_values.csv'))

                self.logger_object.log(self.file_object, "Written null value count to file null_values.csv")

            self.logger_object.log(self.file_object,'Exiting the is_null_present method of the Preprocessor class')

            return null_present

        except OSError as ose:
            self.logger_object.log(self.file_object, f'Error{ose}')
            raise ose

        except Exception as e:
            self.logger_object.log(self.file_object,f"Error: {e}")
            self.logger_object.log(self.file_object,'Exiting the is_null_present method of the Preprocessor class')
            raise e

    def impute_missing_values(self, data):
        """
                    Method Name: impute_missing_values

                    Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                    Output: A Dataframe which has all the missing values imputed.

                    On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object,
                               'Entered the impute_missing_values method of the Preprocessor class')
        try:
            imputer = KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            new_array=imputer.fit_transform(data)  # impute the missing values
            # convert the nd-array from the step above to a Dataframe
            new_data=pd.DataFrame(data=new_array, columns=data.columns)
            self.logger_object.log(self.file_object, 'Imputing missing values Successful.')
            self.logger_object.log(self.file_object,
                                   'Exited the impute_missing_values method of the Preprocessor class')
            return new_data
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occured in impute_missing_values method: {e}')
            self.logger_object.log(self.file_object,
                                   'Exiting the impute_missing_values method of the Preprocessor class ')
            raise e

    def drop_columns_with_zero_std_deviation(self,data):
        """
                        Method Name: get_columns_with_zero_std_deviation

                        Description: This method finds out the columns which have a standard deviation of zero
                        and drops them.

                        On Failure: Raise Exception

                        :param data: DataFrame object
                        :return: cleaned DataFrame
        """
        self.logger_object.log(self.file_object,
                               'Entered the drop_columns_with_zero_std_deviation method of the Preprocessor class')
        try:
            columns = data.columns
            data_n = data.describe()
            col_to_drop = []
            for x in columns:
                # self.logger_object.log(self.file_object, f"Checking column {x} for zero standard deviation.")
                if math.isclose(data_n.loc['std', x], 0):  # check if standard deviation is zero
                    self.logger_object.log(self.file_object, f"Column {x} needs to be dropped.")
                    col_to_drop.append(x)  # prepare the list of columns with standard deviation zero

            if not col_to_drop:
                self.logger_object.log(self.file_object, 'No columns need to be dropped.')
                return data

            self.logger_object.log(self.file_object,
                                   'Column search for Standard Deviation of Zero Successful. Dropping columns.')
            return self.remove_columns(data, col_to_drop)

        except Exception as e:
            self.logger_object.log(self.file_object,f'Error: {e}')
            self.logger_object.log(self.file_object,
                                   'Exiting the drop_columns_with_zero_std_deviation method of the Preprocessor class')
            raise e

    def drop_redundant_settings(self, data):
        '''
        Method Name: drop_redundant_settings

        Description: This method checks if the setting names need to be dropped based on the data.
        If so the columns are dropped and the cleaned dataframe is returned. On failure Exception is raised.

        :param data: DataFrame object
        :return: cleaned DataFrame
        '''
        self.logger_object.log(self.file_object,
                               'Entered the get_redundant_settings method of the Preprocessor class')
        try:
            columns = data.columns
            # it is necessary for grouper to be a list, a ndarray or pandas series will error out
            setting_names = list(columns[2:5])  # this is according to schema file col 3, 4, 5 are settings
            # index_names = ['unit_nr', 'time_cycles']
            rounding = {cols: digits for cols, digits in zip(setting_names, [0, 2, 0])}  # specifies the rounding digit

            # groupby automatically drops na values so na values do not affect this.
            setting_no = data[setting_names].round(rounding).groupby(by=setting_names, dropna=True).size().shape[0]
            self.logger_object.log(self.file_object, f"Found {setting_no} setting(s).")
            if setting_no > 1:
                self.logger_object.log(self.file_object, "No settings columns dropped.")
                return data
            else:
                self.logger_object.log(self.file_object, "Dropping settings columns.")
                return self.remove_columns(data, setting_names)

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error occurred while dropping setting columns: {e}")
            self.logger_object.log(self.file_object, f"Exiting drop_redundant_settings method of {__class__} class.")
            raise e

    def scaleData(self, data):
        '''
        Method Name: scaleData

        Description: This method scales the numerical columns of the given dataframe and outputs the scaled dataframe.

        On Failure: Raise Exception
        :param data: DataFrame
        :return: DataFrame with scaled numerical values
        '''
        self.logger_object.log(self.file_object, f"Starting scaleData method of {__class__} class.")
        try:
            scalar = StandardScaler()

            self.logger_object.log(self.file_object, "Finding all numeric datatype columns.")
            numcol = []
            for cols in data.columns:
                if  is_numeric_dtype(data[cols]):
                    numcol.append(cols)
                else:
                    self.logger_object.log(self.file_object, f"Column {cols} has a non numeric type")

            num_data = data[numcol]
            # non_num_data = [col for col in data.cols and col not in num_data]
            scaled_array = scalar.fit_transform(num_data)

            # data[numcol] = num_data
            scaled_data = pd.DataFrame(scaled_array, columns=numcol, index=data.index)

            data[numcol] = scaled_data
            # final_data = pd.concat([num_data, cat_data], axis=1)
            self.logger_object.log(self.file_object,
                                   f"Data scaled successfully. Exiting scaleData method of {__class__} class.")

            return data

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error occurred while scaling data: {e}")
            self.logger_object.log(self.file_object,
                                   f"Exiting scaleData method of {__class__} class.")
            raise e

    def add_remaining_useful_life(self, data):
        '''
        Method Name: add_remaining_useful_life

        This methods adds the remaining useful life (RUL) column to the given dataframe.

        On Failure: Raise Exception

        :param data: DataFrame
        :return: DataFrame with remaining useful life column
        '''
        self.logger_object.log(self.file_object, f"Starting add_remaining_useful_life method of {__class__} class.")
        try:
            # find max time cycle for each unit
            group_unit = data.groupby('unit_nr')['time_cycles']

            for unit, slice in group_unit:
                rul = slice.max() - slice
                # clip the rul to 125 as we saw in EDA
                rul.clip(upper=125, inplace=True)
                data.loc[data['unit_nr'] == unit, 'RUL'] = rul

            else:
                self.logger_object.log(self.file_object, f"Successfully added remaining useful life column to data.")
                return data

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error occurred while adding RUL column: {e}")
            self.logger_object.log(self.file_object,
                                   f"Exiting add_remaining_useful_life method of {__class__} class.")
            raise e
    def drop_sensor(self,data,filename):
      """

      """
      self.logger_object.log(self.file_object, f"Starting drop_sensor method of {__class__} class.")
      try:
        d={"1":['sensor_01','sensor_05','sensor_06','sensor_10','sensor_16','sensor_18','sensor_19'],
           "2":[],
           "3":['sensor_01','sensor_05','sensor_16','sensor_18','sensor_19'],
           "4":['sensor_01', 'sensor_05', 'sensor_06', 'sensor_10', 'sensor_16', 'sensor_18', 'sensor_19']}
        data=data.drop(d[filename[-1]],axis=1)
        return data

      except Exception as e:
        self.logger_object.log(self.file_object,"Failed at drop_column")
        self.logger_object.log(self.file_object,f"Error: failed at drop_column:{e}")


        raise e

    def select_last_rul(self, data):
        '''
        This module selects the last row for reach engine unit and drops the time_cycles columns,
        the rul is predicted for the last time cycle of engine. Resulting DataFrame has unit_nr as index.

        :param data: DataFrame
        :return: Modified DataFrame
        '''
        try:
            self.logger_object.log(self.file_object, f"Start select_last_rul of {__class__}")
            data = data.groupby('unit_nr').last().drop(['time_cycles'], axis=1)
            return data

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error occurred in select_last_rul: {e}")
            raise e








