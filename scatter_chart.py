from plotly import express as px
import plotly.graph_objects as go
import pandas as pd
import datetime as dt

pkl_data_filepath = r'c:\temp\getTraderCurveTS.pkl'
df = pd.read_pickle(pkl_data_filepath)

x = '[RBOB FUTURE Apr22/May22]'
y = '[RBOB FUTURE May22/Jun22/Jul22 fly]'

start_date = dt.date(2022, 2, 1)
start_date = pd.to_datetime(start_date)
end_date = dt.date(2022, 3, 31)
end_date = pd.to_datetime(end_date)


mask = (df.index > start_date) & (df.index < end_date)
filtered_df = df[mask]
filtered_df.reset_index(inplace=True)

# scale the datetime
time_in_sec = filtered_df.index.astype('int64')
min_sec = time_in_sec.min()
max_sec = time_in_sec.max()
min_marker_size = 5
max_marker_size = 15
marker_sizes = ((time_in_sec - min_sec) * (max_marker_size - min_marker_size) / (max_sec - min_sec)) + min_marker_size
marker_sizes = marker_sizes.to_list()
marker_sizes[-1] = 0

layout = go.Layout(showlegend=True)
fig2 = go.Figure(layout=layout)
marker_trace = go.Scatter(x=filtered_df[x], y=filtered_df[y],
                          mode='markers',
                          marker={'color': time_in_sec,
                                  'colorscale': 'reds',
                                  'size': marker_sizes},
                          name='2022',
                          hovertemplate=
                          '<b>date</b>: %{text}<br>' +
                          '<b>spread</b>: %{x}<br>' +
                          '<b>fly</b>: %{y}<br>',
                          text=filtered_df['time'].dt.strftime('%d-%b-%Y').values,
                          )
line_trace = go.Scatter(x=filtered_df[x], y=filtered_df[y],
                          mode='lines'
                          )
now_trace = go.Scatter(x=[filtered_df.iloc[-1][x]], y=[filtered_df.iloc[-1][y]],
                       mode='markers',
                       marker_symbol='triangle-up',
                       'size')
fig2.add_trace(marker_trace)
fig2.add_trace(line_trace)
fig2.add_trace(now_trace)
fig2.show()
