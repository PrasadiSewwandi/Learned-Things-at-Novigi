import pandas as pd
url='http://localhost:3000/user/'
data = pd.read_csv(url,sep=";") # use sep="," for coma separation. 
data.describe()