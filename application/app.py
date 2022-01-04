import os
from flask import Flask, render_template, request
import json
import plotly
from correlation_charts import create_chart_figure

cwd = os.getcwd()
this_file = os.path.join(cwd, __name__)
app = Flask(this_file, template_folder='templates')


@app.route('/callback', methods=['POST', 'GET'])
def cb():
   return build_data(request.args.get('mydata'))


@app.route('/')
def index():
   return render_template('chartsajax.html', graphJSON=build_data())


def build_data(contract='202110 minus 202111'):
   fig = create_chart_figure(contract=contract)
   graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
   return graphJSON


if __name__ == '__main__':
   app.run(debug=True)
