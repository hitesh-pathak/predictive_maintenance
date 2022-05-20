"""
This is the Entry point for Training the Machine Learning Model.
"""
# Doing the necessary imports
import os
import pandas as pd
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from file_operations import file_methods
from application_logging import logger


class prediction:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        if not os.path.isdir('Prediction_Logs'):
            os.makedirs('Prediction_Logs')
        self.file_object = open("Prediction_Logs/ModelPredictionLog.txt", 'a+')
        self.log_writer.log(self.file_object, f"Created an instance of {__class__} class.")

        self.file_object.close()

    def predictionFromModel(self):
        with open("Prediction_Logs/ModelPredictionLog.txt", 'a+') as self.file_object:
            self.log_writer.log(self.file_object, 'Start of Prediction')
            try:
                # Getting the data from the source
                self.log_writer.log(self.file_object, 'Start data ingestion.')
                data_getter = data_loader.Data_Getter(self.file_object, self.log_writer, mode='predict')
                datagen = data_getter.get_data()
                self.log_writer.log(self.file_object, 'Data ingestion completed.')

                for data, filename in datagen():

                    filename = filename.split('.')[0]  # redefine filename without the .csv part!
                    """doing the data preprocessing"""
                    self.log_writer.log(self.file_object, f"Loaded data from {filename}.")

                    self.log_writer.log(self.file_object, "Initialize Preprocessor class.")
                    preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)

                    self.log_writer.log(self.file_object, "Dropping redundant setting columns.")
                    data = preprocessor.drop_redundant_settings(data)

                    self.log_writer.log(self.file_object, "Dropping sensor columns acc to data visualisation/eda.")
                    data = preprocessor.drop_sensor(data, filename)

                    self.log_writer.log(self.file_object, "Dropping columns with zero standard deviation.")
                    data = preprocessor.drop_columns_with_zero_std_deviation(data)   

                    # impute null values
                    self.log_writer.log(self.file_object, "Checking data for null values.")
                    if preprocessor.is_null_present(data, filename):
                        self.log_writer.log(self.file_object,
                                            "Data contains columns with null values, imputing null values")

                        data = preprocessor.impute_missing_values(data)
                    else:
                        self.log_writer.log(self.file_object, "No columns with null values found in data.")

                    # select last rul
                    self.log_writer.log(self.file_object, "Select last RUL row for test data.")
                    data = preprocessor.select_last_rul(data)

                    # load kmeans model
                    self.log_writer.log(self.file_object, "Loading kmeans model.")
                    file_loader = file_methods.File_Operation(self.file_object, self.log_writer, filename)
                    kmeans = file_loader.load_model('KMeans')

                    # add cluster to data
                    self.log_writer.log(self.file_object, "Adding cluster number to each row of data.")
                    cluster = kmeans.predict(data)
                    data['cluster'] = cluster

                    # empty dataframe
                    df = pd.DataFrame()
                    for c in data['cluster'].unique():
                        cluster_data = data[data['cluster'] == c]
                        cluster_data = cluster_data.drop(['cluster'], axis=1)

                        # finding model
                        self.log_writer.log(self.file_object, f'Finding model for cluster {c}')
                        model_name = file_loader.find_correct_model_file(c)

                        model = file_loader.load_model(model_name)

                        # scale data
                        self.log_writer.log(self.file_object, "Scaling numerical data")
                        cluster_data = preprocessor.scaleData(cluster_data)

                        rul = model.predict(cluster_data)
                        cluster_data['RUL'] = rul

                        # append data
                        df = pd.concat([df, cluster_data])
                        self.log_writer.log(self.file_object, f"Computed RUL value for cluster {c}")

                    else:
                        data = data.join(df['RUL'])
                        file_loader.save_prediction(data['RUL'], filename)
                        self.log_writer.log(self.file_object, f" Saved predictions for file {filename}")
                else:
                    self.log_writer.log(self.file_object, "Predictions saved for all files.")

            except Exception as e:
                self.log_writer.log(self.file_object, f'Error: {e}')
                self.log_writer.log(self.file_object, '!! Unsuccessful End of Training !!')
                raise e
