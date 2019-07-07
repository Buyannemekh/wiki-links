import dash
import dash_core_components as dcc
import dash_html_components as html

# import os, sys
# import psycopg2
#
#
# os.environ["POSTGRES_HOSTNAME"] = sys.argv[1]
# os.environ["POSTGRES_USER"] = sys.argv[2]
# os.environ["POSTGRES_PASSWORD"] = sys.argv[3]
# os.environ["POSTGRES_DBNAME"] = sys.argv[4]
#
# user = os.environ["POSTGRES_USER"]
# host = os.environ["POSTGRES_HOSTNAME"]
# password = os.environ["POSTGRES_PASSWORD"]
# dbname = os.environ["POSTGRES_DBNAME"]
#
# # Settings for psycopg Postgres connector
# con = psycopg2.connect(database=dbname, user=user, password=password, host=host)


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
        html.A('About', href='https://github.com/Buyannemekh',
               target='_blank',
               style=style_header_button),
        dcc.Link('Contact', href='/contact',
                 style=style_header_button
                 ),
        html.A('Resume', href='https://platform.insightdata.com/projects?keyword=2019B.DE.NY',
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
    dcc.Link(html.Button("I'm feeling lucky", id='submit', style=style_button),
             href='/page-3'),

])


page_1_layout = html.Div([
    html.H1('Page 1'),
    html.Br(),

    html.Div(dcc.Input(id='input-box', type='text')),
    html.Br(),

    html.Button('Submit', id='button'),
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
    return 'The input value was "{}" and the button has been clicked {} times'.format(
        value,
        n_clicks
    )


page_2_layout = html.Div([
    # current_count,
    #
    # html.H5("Pick the time frame that you are interested in:"),
    # datepick, tables, graphs,

    dcc.Link('Go to Page 1', href='/page-1'),
    html.Br(),
    dcc.Link('Go back to home', href='/')
])

page_3_layout = html.Div([

    # html.H6("Hey, you got '{}'!".format(df_random_page_title)),
    # html.Br(),
    #
    # html.A("Click here to read about '{}'".format(df_random_page_title),
    #        href="https://en.wikipedia.org/?curid={}".format(df_random_page_id),
    #        target="_blank"),
    # html.Br(),
    html.Div(id='output-random-button',
             children='Enter a value and press submit'),
    html.Br(),

    dcc.Link('Go to Page 1', href='/page-1'),
    html.Br(),

    dcc.Link('Go back to home', href='/')
])


@app.callback(
    dash.dependencies.Output('output-random-button', 'children'),
    [dash.dependencies.Input('submit', 'n_clicks')])
def update_output(n_clicks):
    return 'The input value was and the button has been clicked {} times'.format(
        n_clicks
    )


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
    app.run_server(debug=True, host='0.0.0.0', port=8050)



