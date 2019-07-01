import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import psycopg2
import pandas as pd
import dash_table
import os
import sys


os.environ["POSTGRES_HOSTNAME"] = sys.argv[1]
os.environ["POSTGRES_USER"] = sys.argv[2]
os.environ["POSTGRES_PASSWORD"] = sys.argv[3]
os.environ["POSTGRES_DBNAME"] = sys.argv[4]

# Settings for psycopg Timescale connector
user = os.environ["POSTGRES_USER"]
host = os.environ["POSTGRES_HOSTNAME"]
password = os.environ["POSTGRES_PASSWORD"]
dbname = os.environ["POSTGRES_DBNAME"]
con = psycopg2.connect(database=dbname, user=user, password=password, host=host)


# Query database to load landing page graph
# sql_query_0 = "SELECT link, COUNT(*) FROM pages_links " + \
#               "WHERE time_stamp BETWEEN '2019-06-01' AND CURRENT_TIMESTAMP " + \
#               "GROUP BY link ORDER BY COUNT(*) DESC LIMIT 20"

sql_tot_pages = "SELECT COUNT(*) FROM pages;"
d= {'col1': ["Totol number of pages"], 'col2': [sql_tot_pages]}
df = pd.DataFrame(data=d)

sql_query_0 = "SELECT DATE_TRUNC('month', time_stamp) AS month, + COUNT(*) AS frequency " + \
              "FROM pages GROUP BY month ORDER BY month;"

query_results_0 = pd.read_sql_query(sql_query_0, con)
links = []
for i in range(0, query_results_0.shape[0]):
    links.append(dict(time=query_results_0.iloc[i]['month'], frequency=query_results_0.iloc[i]['frequency']))


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict('records'),
)

current_count = html.Div([dcc.Graph(
        id='example-graph',
        figure={
            'data': [{'x': query_results_0['month'],
                      'y': query_results_0['frequency'],
                      'type': 'line', 'name': 'updated'}],
            'layout': {
                'title': 'How up to date is Wikipedia?',
                'titlefont': {'size': 35},
            }
        }
    )])

datepick = html.Div([dcc.DatePickerSingle(
                            id='datepicker',
                            min_date_allowed = dt(2016, 7, 1),
                            max_date_allowed = dt(2017, 6, 30),
                            initial_visible_month = dt(2016, 11, 1))
                    ],
                    style = {'width': '100%', 'display': 'inline-block'})


app.layout = html.Div(children=[
    html.H1(children='Wiki Links',
            style={'textAlign': 'center',
                   'margin-top': '80px',
                   'margin-bottom': '80px'
                   }),
    current_count,

    html.H5("Pick the date you are interested in:"),
    datepick

])


# Run with `sudo python app2.py` for port 80 (needs sudo permission)
if __name__ == '__main__':
    # app.run_server(debug=True, host='0.0.0.0', port=80)
    app.run_server(debug=True, host='0.0.0.0', port=8050)

