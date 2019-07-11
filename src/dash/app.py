import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
from datetime import datetime as dt
import os, sys
import psycopg2
import pandas as pd


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
app.config.suppress_callback_exceptions = True

style_header = {'height': '5vh',
                'line-height': '4vh',
                'background-color': '#007fff',
                'border-bottom': 'thin lightgrey solid',
                'list-style-type': 'none',
                'margin': '0',
                'padding': '0',
                'overflow': 'hidden',
                'font-family': '"Open Sans", times, sans-serif'}

style_header_button = {'display': 'block',
                       'float': 'left',
                       'padding': '.5vh 2vh',
                       'text-align': 'center',
                       'font-size': '2vh',
                       'color': 'white',
                       'text-decoration': 'none'}

style_button = {'display': 'block',
                'margin': '0 auto',
                'height': '38px',
                'padding': '0 30px',
                'color': '#333',
                'width': '22%',
                'text-align': 'center',
                'font-size': '11px',
                'font-weight': '600',
                'line-height': '38px',
                'letter-spacing': '.1rem',
                'text-transform': 'uppercase',
                'text-decoration': 'none',
                'white-space': 'nowrap',
                'background-color': 'transparent',
                'border-radius': '4px',
                'border': '1px solid #bbb',
                'cursor': 'pointer',
                'box-sizing': 'border-box'}

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        dcc.Link('Home', href='/',
                 style=style_header_button
                 ),
        html.A('LinkedIn', href='https://www.linkedin.com/in/buyan-munkhbat/',
               target='_blank',
               style=style_header_button),
        html.A('GitHub', href='https://github.com/Buyannemekh/wiki-links',
               style=style_header_button),
        html.A('Res', href='https://platform.insightdata.com/projects?keyword=2019B.DE.NY',
               target='_blank',
               style=style_header_button
               ),
        dcc.Link('UpdatePages', href='/',
                 style={'class': 'active',
                        'display': 'block',
                        'float': 'right',
                        'padding': '.5vh 2vh',
                        'text-align': 'center',
                        'font-size': '2vh',
                        'color': 'white',
                        'text-decoration': 'none'})
    ], style=style_header),
    html.Div(id='page-content'),
])


index_page = html.Div([
    html.H1(children='Update Pages on Wikipedia',
            style={'textAlign': 'center',
                   'margin-top': '80px',
                   'margin-bottom': '80px'
                   }),

    html.Img(src="https://img.icons8.com/color/48/000000/search-more.png",
             style={'display': 'block', 'margin': '0 auto'}),
    html.Br(),

    dcc.Link(html.Button('Search by an article',
                         style=style_button),
             href='/page-1'),
    html.Br(),

    dcc.Link(html.Button('Search by date', style=style_button),
             href='/page-2'),

    html.Br(),
    dcc.Link(html.Button("I'm feeling lucky", id='lucky-button', style=style_button),
             href='/page-3'),

])


page_1_layout = html.Div([
    html.H1('Page 1'),
    html.Br(),

    html.Div(dcc.Input(id='input-box', type='text')),
    html.Br(),

    html.Button('Submit', id='button'),
    html.Br(),
    html.Br(),

    html.Div(id='output-container-button',
             children='Enter a value and press submit'),
    html.Br(),

    dcc.Link('Go to Page 2', href='/page-2'),
    html.Br(),

    dcc.Link('Go back to home', href='/'),
])


@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(n_clicks, value):
    if value is not None:
        sql = "SELECT page_id, page_title, time_stamp, link_cnt, count FROM pages_in_out WHERE page_title = " \
              + "'" + value + "';"
        df_page_search = pd.read_sql_query(sql, con)

        if df_page_search.shape[0] == 0:
            return 'Article named "{}" not found.'.format(value)
        else:
            sql_link = "SELECT  links FROM pages_in_out WHERE page_title = " + "'" + value + "';"
            df_page_links = pd.read_sql_query(sql_link, con)

            links = df_page_links['links'][0][:100];
            str_links = ', '.join(links)

            df_page_search.columns = ["ID", "Title", "Last Edited", "Number of Hyperlinks", "Number of Incoming Links"]

            dt_page = dash_table.DataTable(data=df_page_search.to_dict('records'),
                                           columns=[{"name": i, "id": i} for i in df_page_search.columns],
                                           style_cell={
                                               'font_family': 'sans-serif',
                                               'font_size': '20px',
                                               'text_align': 'center'
                                           })

            return dt_page, str_links
    else:
        return 'Please enter Wikipedia article name.'


# Monthly frequency of revisions
sql_query_all_time = "SELECT DATE_TRUNC('month', time_stamp) AS month, + COUNT(*) AS frequency " + \
              "FROM pages GROUP BY month ORDER BY month;"


# Query results
query_results_0 = pd.read_sql_query(sql_query_all_time, con)

current_count = html.Div([dcc.Graph(
        id='example-graph',
        figure={
            'data': [{'x': query_results_0['month'],
                      'y': query_results_0['frequency'],
                      'type': 'line', 'name': 'updated'}],
            'layout': {
                'yaxis': {'title': "Number of articles"},
                'xaxis': {'title': "Time"},
                'title': 'How up to date is Wikipedia?',
                'titlefont': {'size': 35},
            }
        }
    )])

datepick = html.Div([dcc.DatePickerRange(
                            id='my-date-picker-range',
                            min_date_allowed=dt(2008, 1, 1),
                            max_date_allowed=dt.today(),
                            initial_visible_month=dt(2010, 1, 1),
                            start_date=dt(2008, 1, 1),
                            end_date=dt(2019, 7, 1)),
                    ],
                    style={'width': '100%', 'display': 'inline-block'})

graphs = html.Div(id='graphSelection',
                  style={'width': '800', 'display': 'inline-block'},
                  className='selectedTimeframe')


tables = html.Div(id='toptable',
                  style={'width': '800', 'display': 'inline-block'},
                  className='selectedTimeframe')

page_2_layout = html.Div([
    current_count,

    html.H5("Pick the time frame that you are interested in:"),
    datepick, tables, graphs,

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
        sql = "SELECT page_id, page_title, time_stamp, link_cnt, count FROM pages_in_out WHERE time_stamp BETWEEN " + \
              start_date_string + " AND " + end_date_string + " ORDER BY count DESC  LIMIT 20;"
        df_page = pd.read_sql_query(sql, con)
        df_page.columns = ["ID", "Title", "Last Edited", "Number of Hyperlinks", "Number of Incoming Links"]
        # print(df_page)
        # df_page['page_title'] = df_page.apply(lambda row: '<a href="https://en.wikipedia.org/?curid={0}">{1}</a>'
        #                                       .format(row['page_id'], row['page_title']), axis=1)
        return dash_table.DataTable(data=df_page.to_dict('records'),
                                    columns=[{"name": i, "id": i} for i in df_page.columns],
                                    style_cell={
                                        'font_family': 'sans-serif',
                                        'font_size': '20px',
                                        'text_align': 'center'
                                    })
    else:
        print('date not selected!')
        return 'Please enter valid timeframe.'



@app.callback(
    Output('graphSelection', 'children'),
    [Input('my-date-picker-range', 'start_date'), Input('my-date-picker-range', 'end_date')]
)
def display_graphs(start_date, end_date):
    df_frequency_by_day = None
    if start_date is not None and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime("'%Y-%m-%d'")
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime("'%Y-%m-%d'")

        sql = "SELECT DATE_TRUNC('day', time_stamp) AS day, COUNT(*) AS frequency " + \
              "FROM pages WHERE time_stamp BETWEEN" + start_date_string +" AND " + end_date_string + \
              " GROUP BY day ORDER BY day LIMIT 20;"
        df_frequency_by_day = pd.read_sql_query(sql, con)

    return dcc.Graph(
        figure={
            'data': [{'x': df_frequency_by_day["day"],
                      'y': df_frequency_by_day["frequency"],
                      'type': 'bar-line'
                      }],
            'layout': {
                'yaxis': {'title': "Number of articles"},
                'xaxis': {'title': "Time"},
                'title': "Number of articles that are last updated per day"
            }
        }
    )


sql_random_page = "SELECT page_id, page_title FROM pages ORDER BY RANDOM() LIMIT 1"
df_random_page = pd.read_sql_query(sql_random_page, con)
df_random_page_id = df_random_page['page_id'][0]
df_random_page_title = df_random_page['page_title'][0]

page_3_layout = html.Div([
    html.H1('Page 3'),
    html.Br(),

    html.H6("You got '{}'!".format(df_random_page_title)),
    html.Br(),

    html.A("Click here to read about '{}'".format(df_random_page_title),
           href="https://en.wikipedia.org/?curid={}".format(df_random_page_id),
           target="_blank"),
    html.Br(),

    dcc.Link('Go to Page 1', href='/page-1'),
    html.Br(),

    dcc.Link('Go back to home', href='/')
])


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    elif pathname == '/page-2':
        return page_2_layout
    elif pathname == '/page-3':
        return page_3_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)



