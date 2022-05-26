from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np


class Model_Finder:
    """
                This class shall  be used to find the model with best RMSE and R2 score.

    """

    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.rf_model = RandomForestRegressor()
        self.xgb_model = XGBRegressor()

    def get_best_params_for_random_forest(self,train_x,train_y):
        """
                    Method Name: get_best_params_for_random_forest

                    Description: get the parameters for Random Forest Algorithm which give the best accuracy.
                                 Use Hyper Parameter Tuning.

                    Output: The model with the best parameters

                    On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object,
                               f'Entered the get_best_params_for_random_forest method of {__class__}')
        try:
            # initializing with different combination of parameters
            param_grid = {"n_estimators": [10, 50, 100, 130], "max_depth": [6, 8, 10, 12],
                          "min_samples_split" : [60, 70, 80, 100], "min_samples_leaf" : [30, 40, 50, 60]}

            # Creating an object of the Grid Search class
            self.logger_object.log(self.file_object, "Getting best parameter for model with grid search.")
            grid = GridSearchCV(estimator=self.rf_model, param_grid=param_grid, cv=5,  verbose=3,n_jobs=-1)
            # finding the best parameters
            grid.fit(train_x, train_y)
            self.logger_object.log(self.file_object, "Estimated required paramters.")

            # extracting the best parameters
            min_samples_leaf = grid.best_params_['min_samples_leaf']
            max_depth = grid.best_params_['max_depth']
            min_samples_split = grid.best_params_['min_samples_split']
            n_estimators = grid.best_params_['n_estimators']

            # creating a new model with the best parameters
            self.logger_object.log(self.file_object, 'Training random forest regressor with best parameters.')
            rf_model = RandomForestRegressor(n_estimators=n_estimators, min_samples_leaf=min_samples_leaf,
                                              max_depth=max_depth, min_samples_split=min_samples_split)
            # training the mew model
            rf_model.fit(train_x, train_y)
            self.logger_object.log(self.file_object, 'Training complete!')
            self.logger_object.log(self.file_object, f'Random Forest best params: {grid.best_params_}')
            self.logger_object.log(self.file_object,
                                   f'Exited the get_best_params_for_random_forest method of {__class__}')
            return rf_model

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error ocuured while model training: {e}")
            self.logger_object.log(self.file_object,
                                   f'Exited the get_best_params_for_random_forest method of {__class__}')
            raise e

    def get_best_params_for_xgboost(self,train_x,train_y):

        """
                            Method Name: get_best_params_for_xgboost

                            Description: get the parameters for XGBoost Algorithm which give the best accuracy.
                                         Use Hyper Parameter Tuning.

                            Output: The model with the best parameters

                            On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object,
                               f'Entered the get_best_params_for_xgboost method of {__class__}')
        try:
            # initializing with different combination of parameters
            param_grid_xgboost = {

                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth': [2, 4, 5, 6, 8],
                'n_estimators': [10, 50, 80, 100, 120],
                "min_samples_split" : [50,70,100,150],
                "min_samples_leaf" : [20,30,40,50],

            }
            # Creating an object of the Grid Search class

            self.logger_object.log(self.file_object, "Getting best parameter for model with grid search.")
            grid= GridSearchCV(self.xgb_model,param_grid_xgboost, verbose=3,cv=5,n_jobs=-1)
            # finding the best parameters
            grid.fit(train_x, train_y)
            self.logger_object.log(self.file_object, "Grid search for parameters complete.")

            # extracting the best parameters
            learning_rate = grid.best_params_['learning_rate']
            max_depth = grid.best_params_['max_depth']
            n_estimators = grid.best_params_['n_estimators']
            min_samples_split = grid.best_params_['min_samples_split']
            min_samples_leaf = grid.best_params_['min_samples_leaf']

            # creating a new model with the best parameters
            self.logger_object.log(self.file_object, 'Training XGBoost regressor with best parameters.')
            xgb_model = XGBRegressor(learning_rate = learning_rate, max_depth=max_depth, n_estimators=n_estimators,
                                          min_samples_split = min_samples_split, min_samples_leaf=min_samples_leaf)
            # training the mew model
            xgb_model.fit(train_x, train_y)
            self.logger_object.log(self.file_object, 'Training complete!')
            self.logger_object.log(self.file_object, f'XGBoost best params: {grid.best_params_}')
            self.logger_object.log(self.file_object,
                                   f'Exited the get_best_params_for_xgboost method of {__class__}')
            return xgb_model

        except Exception as e:
            self.logger_object.log(self.file_object, f"Error ocuured while model training: {e}")
            self.logger_object.log(self.file_object,
                                   f'Exited the get_best_params_for_xgboost method of {__class__}')
            raise e

    def get_best_model(self,train_x,train_y,test_x,test_y):
        """
                            Method Name: get_best_model
                            Description: Find out the Model which has the best R2 score.
                            Output: The best model name and the model object
                            On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object,
                               f'Entered the get_best_model method of {__class__}')

        self.logger_object.log(self.file_object, "Starting training for XGBoost regressor.")
        # create best model for XGBoost
        xgboost= self.get_best_params_for_xgboost(train_x,train_y)
        self.logger_object.log(self.file_object, "Starting training for Random Forest regressor.")
        # create best model for Random Forest
        random_forest = self.get_best_params_for_random_forest(train_x, train_y)

        try:
            self.logger_object.log(self.file_object, "Evaluating XGBoost prediction and calculating score.")
            prediction_xgboost = xgboost.predict(test_x)  # Predictions using the XGBoost Model

            xgboost_score1 = np.sqrt(mean_squared_error(test_y, prediction_xgboost))
            self.logger_object.log(self.file_object, 'RMSE for XGBoost:' + str(xgboost_score1))

            xgboost_score2 = r2_score(test_y, prediction_xgboost)
            self.logger_object.log(self.file_object, 'R2 score for XGBoost:' + str(xgboost_score2))

            self.logger_object.log(self.file_object, "Evaluating Random forest prediction and calculating score.")
            prediction_random_forest = random_forest.predict(test_x)  # prediction using the RF

            random_forest_score1 = np.sqrt(mean_squared_error(test_y, prediction_random_forest))
            self.logger_object.log(self.file_object, f'RMSE for RF: {random_forest_score1}')

            random_forest_score2 = r2_score(test_y, prediction_random_forest)
            self.logger_object.log(self.file_object, f'R2 score for RF: {random_forest_score2}')

            # comparing the two models
            if random_forest_score2 < xgboost_score2:
                self.logger_object.log(self.file_object, 'Based on scores XGBoost model is selected!')
                return 'XGBoost', xgboost
            else:
                self.logger_object.log(self.file_object, 'Based on scores Random Forest model is selected!')
                return 'RandomForest', random_forest

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred while finding the best model for cluster: {e}')
            self.logger_object.log(self.file_object, f'Exited the get_best_model method of {__class__}')
            raise e

