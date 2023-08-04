import pandas as pd
import numpy as np

#V1

def tbg(df):
    print('Modulo tbg executado!\n')
    nCilindros = 16
    setCols = set()
    for i in range(1,nCilindros + 1):
        colCil = 'Engine Exhaust Gas Port ' + str(i) + ' Temperature [Deg. C]'
        if colCil in df.columns:
            setCols.add(colCil)
            df[colCil] = pd.to_numeric(df[colCil], errors='ignore')
    
    if setCols:
        df['Diff_Temp_Cilindro'] =  df.loc[:,setCols].max(axis=1) - df.loc[:,setCols].min(axis=1)

    else:
        df['Diff_Temp_Cilindro'] = np.nan
    
    return df['Diff_Temp_Cilindro']


if __name__ == '__main__':
    print('Você deve executar o arquivo GUI.py do conversor.')
    exit()