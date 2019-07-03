import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import dash_table
import psycopg2
import pandas as pd
import dash_table
from dash.dependencies import Input, Output
import os
import sys

print(dcc.__version__) # 0.6.0 or above is required

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

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Since we're adding callbacks to elements that don't exist in the app.layout,
# Dash will raise an exception to warn us that we might be
# doing something wrong.
# In this case, we're adding the elements through a callback, so we can ignore
# the exception.
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    html.H1(children='Wiki Pages',
            style={'textAlign': 'center',
                   'margin-top': '80px',
                   'margin-bottom': '80px'
                   }),
    dcc.Link('Search by article', href='/page-1',
             style={'textAlign': 'center',
                    'margin-top': '80px',
                    'margin-bottom': '80px'
                   }),
    html.Br(),
    dcc.Link('Search by date', href='/page-2'),
])


page_1_layout = html.Div([
    html.H1('Page 1'),
    html.Div(dcc.Input(id='input-box', type='text')),
    html.Button('Submit', id='button'),
    html.Div(id='output-container-button',
             children='Enter a value and press submit'),

    dcc.Link('Go to Page 2', href='/page-2'),
    html.Br(),
    dcc.Link('Go back to home', href='/'),
])


@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(n_clicks, value):
    sql = "SELECT page_id, page_title, time_stamp, link_cnt FROM pages WHERE page_title = " + value + ";"
    print(sql)
    return 'The input value was "{}" and the button has been clicked {} times'.format(
        value,
        n_clicks
    )


# @app.callback(dash.dependencies.Output('page-1-content','children'),
#               [dash.dependencies.Input('page-1-dropdown', 'value')])
# def page_1_dropdown(value):
#     return 'You have selected "{}"'.format(value)


# Monthly frequency of revisions
sql_query_0 = "SELECT DATE_TRUNC('month', time_stamp) AS month, + COUNT(*) AS frequency " + \
              "FROM pages GROUP BY month ORDER BY month;"


# Query results
query_results_0 = pd.read_sql_query(sql_query_0, con)
links = []
for i in range(0, query_results_0.shape[0]):
    links.append(dict(time=query_results_0.iloc[i]['month'], frequency=query_results_0.iloc[i]['frequency']))

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
                            id='my-date-picker-range',
                            min_date_allowed=dt(2010, 7, 1),
                            max_date_allowed=dt.today(),
                            initial_visible_month=dt(2019, 6, 1),
                            start_date=dt(2018, 1, 1),
                            end_date=dt(2019, 6, 1)),
                    ],
                    style={'width': '100%', 'display': 'inline-block'})


tables = html.Div(id='toptable',
                  style={'width': '50%', 'display': 'inline-block'})

page_2_layout = html.Div([
    current_count,

    html.H5("Pick the date you are interested in:"),
    datepick, tables,

    dcc.Link('Go to Page 1', href='/page-1'),
    html.Br(),
    dcc.Link('Go back to home', href='/')
])


@app.callback(
    Output('toptable', 'children'),
    [Input('my-date-picker-range', 'start_date'), Input('my-date-picker-range', 'end_date')]
)


# Get table of pages information within timeframe
def get_page_table(start_date, end_date):
    df_page = None
    if start_date is not None and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime("'%Y-%m-%d'")
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime("'%Y-%m-%d'")

        sql = "SELECT page_id, page_title, time_stamp, link_cnt FROM pages WHERE time_stamp BETWEEN " + \
              start_date_string + " AND " + end_date_string + " ORDER BY time_stamp LIMIT 10;"
        df_page = pd.read_sql_query(sql, con)
        # print(df_page)
        # df_page['page_title'] = df_page.apply(lambda row: '<a href="https://en.wikipedia.org/?curid={0}">{1}</a>'
        #                                       .format(row['page_id'], row['page_title']), axis=1)
    else:
        print('date not selected!')
    return dash_table.DataTable(data=df_page.to_dict('records'),
                                columns=[{"name": i, "id": i} for i in df_page.columns])


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    elif pathname == '/page-2':
        return page_2_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)