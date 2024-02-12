import pandas as pd
import pytest
from chunk_dataframe import chunk_dataframe

dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:01:00", freq="s")
df = pd.DataFrame({"dt": dfs.repeat(10)})

dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:01:00", freq="s")
df = pd.DataFrame({"dt": dfs.repeat(10)})

def test_valid_column_name():
    chunk_size = 5
    column_name = "dt"
    result_chunks = chunk_dataframe(df, chunk_size, column_name)
    
    assert len(result_chunks) > 0
    for chunk in result_chunks:
        assert column_name in chunk.columns

def test_invalid_column_name():
    with pytest.raises(ValueError):
        chunk_size = 5
        column_name = "invalid_column"
        chunk_dataframe(df, chunk_size, column_name)

def test_valid_column_number():
    chunk_size = 5
    column_number = 0
    result_chunks = chunk_dataframe(df, chunk_size, df.columns[column_number])
    
    assert len(result_chunks) > 0
    for chunk in result_chunks:
        assert df.columns[column_number] in chunk.columns

def test_invalid_column_number():
    with pytest.raises(ValueError):
        chunk_size = 5
        column_number = len(df.columns)
        chunk_dataframe(df, chunk_size, column_number)

def test_chunk_size_greater_than_dataframe_size():
    chunk_size = len(df) + 1
    column_name = "dt"
    result_chunks = chunk_dataframe(df, chunk_size, column_name)

    assert len(result_chunks) == 1
    assert len(result_chunks[0]) == len(df)

def test_empty_dataframe():
    empty_df = pd.DataFrame()
    chunk_size = 5
    column_name = "dt"
    
    with pytest.raises(ValueError):
        result_chunks = chunk_dataframe(empty_df, chunk_size, column_name)
        assert len(result_chunks) == 0