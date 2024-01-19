import pandas as pd

# DataFrame 1
data1 = {'A': ['A0', 'A1', 'A2', 'A3'],
         'B': ['B0', 'B1', 'B2', 'B3']}
df1 = pd.DataFrame(data1)

# DataFrame 2
data2 = {'C': ['C0', 'C1'],
         'D': ['D0', 'D1']}
df2 = pd.DataFrame(data2)

#df1 = df1.set_index('key')
#df2 = df2.set_index('key')


merged_df = df1.join(df2)

merged_df = merged_df.reset_index()

print(merged_df)