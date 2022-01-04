import os
import pandas as pd
from plotly import express as px
from correlation_data import file_for_data, build_self_join_data
from mosaic.constants import DEV, path


def create_chart_figure(contract):
    df = _get_data()
    df = _filter_data(df, contract)
    fig = _create_scatter_matrix(df)
    return fig


def _get_data():
    pathfile = os.path.join(path, file_for_data)
    data_df = pd.read_pickle(pathfile)
    return build_self_join_data(data_df)


def _filter_data(df, contract):
    mask_x = df['contract_x'] == contract
    mask_y = df['contract_y'] == contract
    return df[mask_x & mask_y]


def _create_scatter_matrix(df):
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
