import pandas as pd
df = pd.read_csv("raw_retail.csv", encoding="utf-8")   # or pd.read_excel
df.shape
df.head()
df.info()
df.describe(include='all')

df.to_csv("retail_working.csv", index=False)

df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce')
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

# drop rows missing invoice_no or invoice_date
df = df.dropna(subset=['invoice_no','invoice_date'])
# optionally remove rows with unit_price <= 0
df = df[df['unit_price'] > 0]

full_dups = df.duplicated().sum()
df = df.drop_duplicates()

df['description'] = df['description'].fillna('unknown')
df['customer_id'] = df['customer_id'].fillna('anonymous')

