import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import psycopg2
import pandas as pd


# Settings for psycopg Timescale connector
user = 'postgres'
host = 'ec2-34-239-95-229.compute-1.amazonaws.com'
dbname = 'wikicurrent'
con = psycopg2.connect(database=dbname, user=user, password='$password', host=host)


# Query database to load landing page graph
# sql_query_0 = "SELECT link, COUNT(*) FROM pages_links " + \
#               "WHERE time_stamp BETWEEN '2019-06-01' AND CURRENT_TIMESTAMP " + \
#               "GROUP BY link ORDER BY COUNT(*) DESC LIMIT 20"

sql_query_0 = "SELECT DATE_TRUNC('month', time_stamp) AS month, + COUNT(*) AS frequency " + \
              "FROM pages GROUP BY month LIMIT 10;"

query_results_0 = pd.read_sql_query(sql_query_0, con)
links = []
for i in range(0, query_results_0.shape[0]):
    links.append(dict(time=query_results_0.iloc[i]['month'], frequency=query_results_0.iloc[i]['frequency']))
print(links)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div(children=[
    html.H1(children='Wiki Links',
            style={'textAlign': 'center',
                   'margin-top': '80px',
                   'margin-bottom': '80px'
                   }),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [{'x': query_results_0['month'],
                      'y': query_results_0['frequency'],
                      'type': 'line', 'name': 'updated'}],
            'layout': {
                'title': 'How current are the Wikipedia pages right now?'
            }
        }
    )
])


# Run with `sudo python app2.py` for port 80 (needs sudo permission)
if __name__ == '__main__':
    # app.run_server(debug=True, host='0.0.0.0', port=80)
    app.run_server(debug=True, host='0.0.0.0', port=8050)

