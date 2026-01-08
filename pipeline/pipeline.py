import sys
import pandas as pd


month = int(sys.argv[1] if len(sys.argv) > 1 else 0)

df = pd.DataFrame({"A":[1,2], "B": [3,4]})  # Example usage of pandas to avoid unused import warning
df["month"] = month
print(df.head())
df.to_parquet(f"output_{month}.parquet")

print("Pipeline module loaded successfully. Month argument:", month)  