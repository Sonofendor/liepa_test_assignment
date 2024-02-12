import pandas as pd

def chunk_dataframe(df, chunk_size, column_name):

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the dataframe.")
    

    if isinstance(column_name, int):
        if column_name >= len(df.columns):
            raise ValueError("Invalid column number. Column number exceeds the number of columns in the dataframe.")
    
    chunks = []
    current_chunk_start = 0
    current_chunk_size = 0
    
    for i, row in df.iterrows():
        current_chunk_size += 1
        
        if current_chunk_size == chunk_size:
            chunks.append(df.loc[current_chunk_start:i, [column_name]])
            current_chunk_start = i + 1
            current_chunk_size = 0
    
    if current_chunk_size > 0:
        chunks.append(df.loc[current_chunk_start:, [column_name]])
    
    return chunks