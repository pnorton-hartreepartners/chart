import os
import pandas as pd
from plotly import express as px
from correlation_data import build_self_join_data
from mosaic.constants import DEV, path, file_for_data, file_for_reduced_data


def create_chart_figure(contract):
    df = get_data()
    df = filter_data(df, contract)
    fig = create_scatter_matrix(df)
    return fig


def get_data():
    pathfile = os.path.join(path, file_for_reduced_data)
    return pd.read_pickle(pathfile)


def filter_data(df, contract_x, contract_y=None):
    if not contract_y:
        contract_y = contract_x
    mask_x = df['contract_x'] == contract_x
    mask_y = df['contract_y'] == contract_y
    return df[mask_x & mask_y]


def create_scatter_matrix(df):
    fig = px.scatter(data_frame=df,
                     x='value_x',
                     y='value_y',
                     facet_col='symbol_x', facet_row='symbol_y',
                     trendline='ols')
    return fig


if __name__ == '__main__':
    env = DEV
    contract = '202110 minus 202111'

    pathfile = os.path.join(path, file_for_data)
    data_df = pd.read_pickle(pathfile)

    cartesian_product_df = build_self_join_data(data_df)
    fig = create_chart_figure(cartesian_product_df, contract)
