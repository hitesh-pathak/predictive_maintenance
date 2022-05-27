'''
    Higher level function to fetch database files and generate prediction output.
'''
import os
from application_logging import logger
from prediction_validation_insertion import PredictionValidation
from predict_from_model import Prediction


# define the prediction generator
def gen_prediction(path):
    '''
    This function generates prediction of remaining useful life from the datasets present at path.

    :param path: path containing prediction datasets
    :return: None
    '''

    errors = []

    if not os.path.isdir('Prediction_Logs'):
        os.makedirs('Prediction_Logs')
    # open logs
    log_writer = logger.App_Logger()
    file_object = open("Prediction_Logs/ModelPredictionLog.txt", 'a+', encoding='utf-8')

    try:
        log_writer.log(file_object, 'Importing prediction files.')
        validator = PredictionValidation(path)
        validator.pred_validation()

        log_writer.log(file_object, 'Importing finished. Start Prediction process')
        predictor =  Prediction(file_object, log_writer)
        predictor.prediction_from_model()

    except Exception as Ex:
        errors.append('Unable to generate predictions.')
        return {'error': errors}

    else:
        return 'Predictions saved successfully.'

    finally:
        file_object.close()


