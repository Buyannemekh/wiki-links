import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import psycopg2
import pandas as pd
import dash_table
from dash.dependencies import Input, Output
import os
import sys

## OS
os.environ["POSTGRES_HOSTNAME"] = sys.argv[1]
os.environ["POSTGRES_USER"] = sys.argv[2]
os.environ["POSTGRES_PASSWORD"] = sys.argv[3]
os.environ["POSTGRES_DBNAME"] = sys.argv[4]

user = os.environ["POSTGRES_USER"]
host = os.environ["POSTGRES_HOSTNAME"]
password = os.environ["POSTGRES_PASSWORD"]
dbname = os.environ["POSTGRES_DBNAME"]

# Settings for psycopg Postgres connector
con = psycopg2.connect(database=dbname, user=user, password=password, host=host)

# Style
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


## SQL Queries
sql_tot_pages = "SELECT COUNT(*) FROM pages;"
d= {'col1': ["Totol number of pages"], 'col2': [sql_tot_pages]}
df = pd.DataFrame(data=d)

# Monthly frequency of revisions
sql_query_0 = "SELECT DATE_TRUNC('month', time_stamp) AS month, + COUNT(*) AS frequency " + \
              "FROM pages GROUP BY month ORDER BY month;"

# Get table of pages information within timeframe
def get_rank_table(start_date, end_date, hour, conn):
    sql = "SELECT * FROM pages WHERE time_stamp BETWEEN " + start_date + " AND " + end_date + " " +\
          "ORDER BY time_stamp LIMIT 10;"
    cur = conn.cursor()
    cur.execute(sql)
    col = ['page_id', 'page_title', 'time_stamp', 'links', 'link_cnt']
    df = pd.DataFrame(cur.fetchall(), columns=col)
    conn.commit()
    cur.close()
    return df


## Query results
query_results_0 = pd.read_sql_query(sql_query_0, con)
links = []
for i in range(0, query_results_0.shape[0]):
    links.append(dict(time=query_results_0.iloc[i]['month'], frequency=query_results_0.iloc[i]['frequency']))


## App layout
# app.layout = dash_table.DataTable(
#     id='table',
#     columns=[{"name": i, "id": i} for i in df.columns],
#     data=df.to_dict('records'),
# )

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

datepick = html.Div([dcc.DatePickerRange(
                            id='my-date-picker-range-1',
                            min_date_allowed=dt(2010, 7, 1),
                            max_date_allowed=dt.today(),
                            initial_visible_month=dt(2019, 6, 1),
                            start_date=dt(2018, 1, 1),
                            end_date=dt(2019, 6, 1)),
                    ],
                    style={'width': '100%', 'display': 'inline-block'})


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


# ## Call backs
# @app.callback(
#     Output('toptable', 'children'),
#     [Input('datepicker', 'date'), Input('hourpicker', 'value')]
# )
#
# def display_tables(date, selected_values):
#     # selected_values: hour session(0-23) / daily(24)
#
#     df = None
#     if date is not None:
#         date = dt.strptime(date, '%Y-%m-%d')
#         date_str = date.strftime('%Y%m%d')
#         if selected_values == 24:
#             df = q.get_rank_daily_table(date_str, conn)
#         elif selected_values is not None:
#             df = q.get_rank_table(date_str, selected_values, conn)
#     else:
#         print('date not selected!')
#     print('Successfully get rank table...')
#     return dtab.DataTable(rows = df.to_dict('records'), columns = ['company_id', 'count'])


# Run with `sudo python app2.py` for port 80 (needs sudo permission)
if __name__ == '__main__':
    # app.run_server(debug=True, host='0.0.0.0', port=80)
    app.run_server(debug=True, host='0.0.0.0', port=8050)

