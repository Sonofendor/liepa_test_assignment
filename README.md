# liepa_test_assignment

## Пример использования

```python
import pandas as pd

dfs = pd.date_range(
    "2023-01-01 00:00:00", "2023-01-01 00:01:00", freq="s"
)
df = pd.DataFrame({"dt": dfs.repeat(10)})

chunk_size = 5
column_name = "dt"  # или column_name = 0, если используется номер столбца

try:
    result_chunks = chunk_dataframe_optimized(df, chunk_size, column_name)
    for i, chunk in enumerate(result_chunks):
        print(f"Chunk {i+1}:\n{chunk}\n")
except ValueError as e:
    print(f"Error: {e}")
```