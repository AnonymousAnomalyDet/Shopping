import df_utils
df=df_utils.read_csv('out_total_combined.csv')

print(type(df))
print(df['date_key'][305])
