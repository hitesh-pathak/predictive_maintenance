"""
This is the Entry point for Training the Machine Learning Model.
"""
# Doing the necessary imports
import os
import pandas as pd
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from file_operations import file_methods
from application_logging.logger import App_Logger


class Prediction:
    """
        This class handles the pre-processing and model prediction of data files.
    """

    def __init__(self, path: str):
        self.log_writer = App_Logger()

        # verify that log directory exists
        if not os.path.isdir('Prediction_Logs'):
            os.makedirs('Prediction_Logs')

        file_object = open("Prediction_Logs/ModelPredictionLog.txt", 'a+', encoding='utf-8')

        self.log_writer.log(file_object, f"Created an instance of {__class__} class.")

        self.prediction_path = path

        file_object.close()


    def prediction_from_model(self):
        """
        This method pre processes all the prediction files and generates rul prediction for them

        Returns:
            True: On success, else returns None
        """
        with open("Prediction_Logs/ModelPredictionLog.txt",
                    'a+', encoding='utf-8') as file_object:

            self.log_writer.log(file_object, 'Start of Prediction from models.')
            try:
                # Getting the data from the source
                self.log_writer.log(file_object, 'Start data ingestion.')
                data_getter = data_loader.DataGetter(file_object,
                                                        self.log_writer, mode='predict',
                                                        path=self.prediction_path)
                datagen = data_getter.get_data()
                self.log_writer.log(file_object, 'Data ingestion completed.')

                for data, filename in datagen():

                    filename = filename.split('.')[0]  # redefine filename without the .csv part!

                    self.log_writer.log(file_object, f"Loaded data from {filename}.")

                    self.log_writer.log(file_object, "Initialize Preprocessor class.")
                    preprocessor = preprocessing.Preprocessor(file_object, self.log_writer)

                    self.log_writer.log(file_object, "Dropping redundant setting columns.")
                    data = preprocessor.drop_redundant_settings(data)

                    self.log_writer.log(file_object,
                                        "Dropping sensor columns acc to data visualisation/eda.")
                    data = preprocessor.drop_sensor(data, filename)

                    self.log_writer.log(file_object,
                                        "Dropping columns with zero standard deviation.")
                    data = preprocessor.drop_columns_with_zero_std_deviation(data)

                    # impute null values
                    self.log_writer.log(file_object, "Checking data for null values.")
                    if preprocessor.is_null_present(data, filename):
                        self.log_writer.log(file_object,
                                    "Data contains columns with null values, imputing null values")

                        data = preprocessor.impute_missing_values(data)
                    else:
                        self.log_writer.log(file_object,
                                            "No columns with null values found in data.")

                    # select last rul
                    self.log_writer.log(file_object, "Select last RUL row for test data.")
                    data = preprocessor.select_last_rul(data)

                    # load kmeans model
                    self.log_writer.log(file_object, "Loading kmeans model.")
                    file_loader = file_methods.File_Operation(file_object,
                                                            self.log_writer, filename)
                    kmeans = file_loader.load_model('KMeans')

                    # add cluster to data
                    self.log_writer.log(file_object,
                                        "Adding cluster number to each row of data.")
                    cluster = kmeans.predict(data)
                    data['cluster'] = cluster

                    # empty dataframe
                    df = pd.DataFrame()
                    for cluster in data['cluster'].unique():
                        cluster_data = data[data['cluster'] == cluster]
                        cluster_data = cluster_data.drop(['cluster'], axis=1)

                        # finding model
                        self.log_writer.log(file_object,
                                            f'Finding model for cluster {cluster}')
                        model_name = file_loader.find_correct_model_file(cluster)

                        model = file_loader.load_model(model_name)

                        # scale data
                        self.log_writer.log(file_object, "Scaling numerical data")
                        cluster_data = preprocessor.scaleData(cluster_data)

                        rul = model.predict(cluster_data)
                        cluster_data['RUL'] = rul

                        # append data
                        df = pd.concat([df, cluster_data])
                        self.log_writer.log(file_object,
                                            f"Computed RUL value for cluster {cluster}")


                    data = data.join(df['RUL'])
                    file_loader.save_prediction(data['RUL'], filename)
                    self.log_writer.log(file_object, f" Saved predictions for file {filename}")

                self.log_writer.log(file_object, "Predictions saved for all files.")
                # return true on success
                return True

            except Exception as e:
                self.log_writer.log(file_object, f'Error: {e}')
                self.log_writer.log(file_object, '!! Unsuccessful End of Training !!')
                raise e
