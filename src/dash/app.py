import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash(__name__)

# Header navigation bar
app.layout = html.Div([
    html.Div(dcc.Location(id='url', refresh=False)),
    html.Div([
        dcc.Link('Home', className='header-button', href='/'),
        html.A('About', href='https://github.com/Buyannemekh',
               target='_blank',
               className='header-button'),
        dcc.Link('Contact', className='header-button', href='/contact'),
        html.A('Resume', href='https://platform.insightdata.com/projects?keyword=2019B.DE.NY',
               target='_blank', className='header-button'),
        dcc.Link('UpdatePages', className='header-button', href='/', style={'float': 'right', 'class': 'active'})
    ], className='app-header'),
    html.Div(id='page-content'),
])


index_page = html.Div([
    html.H1(children='Update Pages on Wikipedia',
            style={'textAlign': 'center',
                   'margin-top': '80px',
                   'margin-bottom': '80px'
                   }),

    html.Img(src="https://img.icons8.com/color/48/000000/search-more.png", className='image'),
    html.Br(),

    dcc.Link(html.Button('Search by an article',  className='button1'),
             href='/page-1'),
    html.Br(),

    dcc.Link(html.Button('Search by date', className='button1'),
             href='/page-2'),

    html.Br(),
    dcc.Link(html.Button("I'm feeling lucky", className='button1'),
             href='/page-3'),

])

page_1_layout = html.Div([
    html.Div(dcc.Input(id='input-box', type='text')),
    html.Button('Submit', id='button'),
    html.Div(id='output-container-button',
             children='Enter a value and press submit')
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

# page_1_layout = html.Div([
#     html.H1('Search by an article name'),
#     html.Br(),
#
#     html.Div(dcc.Input(id='input-box', type='text')),
#     html.Br(),
#
#     html.Button('Submit', id='button'),
#     html.Br(),
#
#     html.Div(id='output-container-button',
#              children='Enter a value and press submit'),
#     html.Br(),
#
#     dcc.Link('Go to Page 2', href='/page-2'),
#     html.Br(),
#
#     dcc.Link('Go back to home', href='/'),
# ])

#
# page_1_layout = html.Div([
#     html.Div(dcc.Input(id='input-box', type='text')),
#     html.Div(id='output-container-button',
#              children='Enter a value and press submit')
# ])
#
#
# @app.callback(
#     dash.dependencies.Output('output-container-button', 'children'),
#     [dash.dependencies.Input('input-box', 'value')])
# def update_output(value):
#     return 'The input value was "{}" and the button has been clicked times'.format(
#         value
#     )
#
#
# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    # elif pathname == '/page-2':
    #     return page_2_layout
    # elif pathname == '/page-3':
    #     return page_3_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here


if __name__ == '__main__':
    app.run_server(debug=True)



