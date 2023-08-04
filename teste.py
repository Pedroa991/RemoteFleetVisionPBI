import pandas as pd

x = pd.DataFrame({'a':[0.1,1,66]})

x['a'] = x.loc[x['a']<=1,'a']*100

print(x)