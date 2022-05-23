from flask import Flask, render_template, request
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction


# Initialise Flask app
app = Flask(__name__)

# Set up main route
@app.route('/', methods=['GET', 'POST'])
def main():

    try:
        if request.method == 'POST':
            # Extract the input
            path = request.form.get('filepath')

            print(path)

            # predict based on filepath
            validator = pred_validation(path)

            print(type(validator))
            # validator.pred_validation()
            #
            # # save predictions using models
            # predictor = prediction()
            # predictor.predictionFromModel()

            return render_template("index.html", result = 'Predictions saved successfully.')

        elif request.method == 'GET':
            return render_template('index.html')

    except Exception as e:
        return render_template("index.html", result = f'Error: {e}')
