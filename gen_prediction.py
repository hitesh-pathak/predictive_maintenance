import os
from application_logging import logger
from prediction_Validation_Insertion import PredictionValidation
from predictFromModel import prediction


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
    file_object = open("Prediction_Logs/ModelPredictionLog.txt", 'a+')

    try:
        log_writer.log(file_object, 'Importing prediction files.')
        validator = PredictionValidation(path)
        validator.pred_validation()

        log_writer.log(file_object, 'Importing finished. Start Prediction process')
        predictor =  prediction(file_object, log_writer)
        predictor.predictionFromModel()

    except Exception as e:
        errors.append('Unable to generate predictions.')
        return {'error': errors}

    else:
        return 'Predictions saved successfully.'

    finally:
        file_object.close()


