"""
This is the Entry point for Training the Machine Learning Model.
"""
# Doing the necessary imports
import os
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger


class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        if not os.path.isdir('Training_Logs'):
            os.makedirs('Training_Logs')
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
        self.log_writer.log(self.file_object, f"Created an instance of {__class__} class.")
        self.file_object.close()

    def trainingModel(self):
        with open("Training_Logs/ModelTrainingLog.txt", 'a+') as self.file_object:
            self.log_writer.log(self.file_object, 'Start of Training')
            try:
                # Getting the data from the source
                self.log_writer.log(self.file_object, 'Start data ingestion.')
                data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
                datagen = data_getter.get_data()
                self.log_writer.log(self.file_object, 'Data ingestion completed.')

                for data, filename in datagen():
                    filename = filename.split('.')[0]  # redefine filename without the .csv part!
                    """doing the data preprocessing"""
                    self.log_writer.log(self.file_object, f"Loaded data from {filename}.")

                    self.log_writer.log(self.file_object, "Initialize Preprocessor class.")
                    preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)

                    # add RUL column
                    self.log_writer.log(self.file_object, "Adding remaining useful life column to data.")
                    data = preprocessor.add_remaining_useful_life(data)

                    # drop unit_nr and time_cycles
                    self.log_writer.log(self.file_object, "Dropping unit_nr and time_cycles column")
                    data = preprocessor.remove_columns(data, ['unit_nr', 'time_cycles'])

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

                    X = data.drop(['RUL'], axis=1)
                    Y = data['RUL']

                    """ Applying the clustering approach"""

                    self.log_writer.log(self.file_object, "Initialise data clustering.")
                    kmeans = clustering.KMeansClustering(self.file_object,self.log_writer)
                    number_of_clusters = kmeans.elbow_plot(X, filename)  # elbow plot

                    # Divide the data into clusters
                    X = kmeans.create_clusters(X,number_of_clusters, filename)

                    X['Labels'] = Y

                    # getting the unique clusters from our dataset
                    list_of_clusters = X['Cluster'].unique()

                    """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

                    self.log_writer.log(self.file_object, "Dividing data into seperate clusters for training.")
                    for i in list_of_clusters:
                        cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster

                        # Prepare the feature and Label columns
                        cluster_features = cluster_data.drop(['Labels','Cluster'],axis=1)
                        cluster_label = cluster_data['Labels']

                        # splitting the data into training and test set for each cluster one by one
                        x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label,
                                                                            test_size=1/3, random_state=355)
                        self.log_writer.log(self.file_object, f'Scaling cluster data for cluster {i}')
                        x_train = preprocessor.scaleData(x_train)
                        x_test = preprocessor.scaleData(x_test)

                        self.log_writer.log(self.file_object, f'Initialize model tuning for cluster {i}')
                        model_finder = tuner.Model_Finder(self.file_object, self.log_writer)

                        # getting the best model for each of the clusters
                        best_model_name, best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)

                        # saving the best model to the directory.
                        self.log_writer.log(self.file_object,
                                            f"Model {best_model_name} selected for cluster {i}. Saving model.")

                        file_op = file_methods.File_Operation(self.file_object,self.log_writer, filename)
                        file_op.save_model(best_model,best_model_name+str(i))

                        self.log_writer.log(self.file_object, f'Training complete for Cluster {i}')

                    else:
                        self.log_writer.log(self.file_object, f'Training finished for the data {filename}')
                else:
                    self.log_writer.log(self.file_object, "Training completed for all datasets.")

            except Exception as e:
                self.log_writer.log(self.file_object, '!! Unsuccessful End of Training !!')
