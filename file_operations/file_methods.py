import pickle
import os
import shutil
import re


class File_Operation:
    """
                This class shall be used to save the model after training
                and load the saved model for prediction.
    """
    def __init__(self, file_object, logger_object, filename):
        self.file_object = file_object
        self.logger_object = logger_object
        self.filename = filename
        self.model_directory = 'models'
        self.save_directory = os.path.join(self.model_directory, self.filename)

    def save_model(self, model, modelname):
        """
            Method Name: save_model

            Description: Save the model file to directory

            Outcome: File gets saved

            On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object, f'Entered the save_model method of {__class__}')
        try:
            path = os.path.join(self.save_directory,modelname)  # create separate directory for each cluster
            self.logger_object.log(self.file_object,
                                   'Removing old model directory and creating fresh directory.')
            if os.path.isdir(path):  # remove previously existing models for each clusters
                shutil.rmtree(path)
                os.makedirs(path)
            else:
                os.makedirs(path)  # make it

            self.logger_object.log(self.file_object, f'Trying to save model {modelname}')
            modelfile = os.path.join(path, modelname + '.sav')
            with open(modelfile,'wb') as f:
                pickle.dump(model, f) # save the model to file

            self.logger_object.log(self.file_object, 'Model saved successfully!')
            self.logger_object.log(self.file_object, f'Exited the save_model method of {__class__}')

            return True

        except Exception as ose:
            self.logger_object.log(self.file_object, f"Error occurred while saving model: {ose}")
            self.logger_object.log(self.file_object, f" Exiting the save_model method of {__class__}")
            raise ose

    def load_model(self, modelname):
        """
                    Method Name: load_model

                    Description: load the model file to memory

                    Output: The Model file loaded in memory

                    On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object, f'Entered the load_model method of {__class__}')
        try:
            # find model directory
            regex = re.compile(r"^train_input_00[1-4]$")
            onlydirs = [d for d in os.listdir(self.model_directory)
                        if os.path.isdir(os.path.join(self.model_directory, d)) and regex.match(d)]

            # choose the loading directory inside models/ directory.
            end = self.filename[-1]
            for dir in onlydirs:
                if dir[-1] == end:
                    load_directory = os.path.join(self.model_directory, dir)
                    break
                else:
                    continue
            else:
                error = Exception(f'No valid model load directory found for {self.filename}')
                raise error

            with open(os.path.join(load_directory, modelname, modelname + '.sav'), 'rb') as f:
                self.logger_object.log(self.file_object, f"Loaded model {modelname}.")
                return pickle.load(f)

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred while loading model: {e}.')
            self.logger_object.log(self.file_object, f"Exited load_model method of {__class__}")
            raise e

    def find_correct_model_file(self, cluster):
        """
                            Method Name: find_correct_model_file

                            Description: Select the correct model based on cluster number

                            Output: The Model file

                            On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object, f'Entered the find_correct_model_file method of {__class__}')
        try:
            if type(cluster) == int:
                error = TypeError("cluster must be an integer value")
                raise error

            # find the model directory
            regex = re.compile(r"^train_input_00[1-4]$")
            onlydirs = [d for d in os.listdir(self.model_directory)
                        if os.path.isdir(os.path.join(self.model_directory, d)) and regex.match(d)]

            # find the models save directory for this filename
            end = self.filename[-1]
            for dir in onlydirs:
                if dir[-1] == end:
                    load_directory = os.path.join(self.model_directory, dir)
                    break
                else:
                    continue
            else:
                error = Exception(f'No valid model load directory found for cluster {self.filename}')
                raise error

            # choose the model inside the models/train_input directories.
            models = [m for m in os.listdir(load_directory)
                      if os.path.isdir(os.path.join(load_directory, m)) and m.endswith(str(cluster))]

            if len(models) != 1:
                error = Exception('More than one models found. Please check model directory for duplicates.')
                raise error

            modelname = models[0]
            self.logger_object.log(self.file_object, f"Model {modelname} found for cluster {cluster}")
            return modelname

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f"Error occurred while finding model for cluster {cluster}.")
            self.logger_object.log(self.file_object, f"Exiting find_correct_model_file method of {__class__}")
            raise e

    def save_prediction(self, data, filename):
        '''
            This method saves the given DataFrame as a csv file. If the file already exists in save location it is
            removed.

        :param data: DataFrame
        :param filename: Filename corresponding to DataFrame
        :return: True on success, None otherwise
        '''
        try:
            self.logger_object.log(self.file_object, f"Entered save_prediction method of {__class__}")
            end = filename[-1]
            pred_file = 'prediction_output_00' + end + '.csv'
            path = os.path.join('Prediction_output', pred_file)

            self.logger_object.log(self.file_object, "Creating output directory.")
            if not os.path.isdir('Prediction_output'):
                os.makedirs('Prediction_output')
            else:
                # removing old prediction files
                if os.path.isfile(path):
                    self.logger_object.log(self.file_object, 'Removing existing prediction file')
                    os.remove(path)

            # save prediction to csv file
            data.to_csv(path, mode='w', index=True)
            self.logger_object.log(self.file_object, f"Prediction saved to file {pred_file}")
            self.logger_object.log(self.file_object, f"Exiting save_prediction method of {__class__}")


            return True

        except OSError as ose:
            self.logger_object(self.file_object, f"Error occurred in save_prediction method of {__class__}")
            raise ose




