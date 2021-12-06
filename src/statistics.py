import pandas as pd

df = pd.read_csv('../output/outputSpark_full_index.csv')

# print(df.head(10))
print("Pocet zaznamov", df.shape[0])
print("Pocet datumov narodenia s a bez presneho datumu (doplnila som nieco)", df['Birth note'].value_counts())
print("Pocet datumov umrtia bez presneho datumu (doplnila som nieco)", df['Death note'].value_counts())

print("Pocet datumov umrtia bez datumu", df['Death date'].value_counts(None))
print("Pocet datumov narodenia bez datumu", df['Birth date'].value_counts(None))

