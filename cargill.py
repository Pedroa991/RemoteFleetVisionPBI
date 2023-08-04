#Cargil v1.1

import tempfile
import zipfile
import pandas as pd
import os
import shutil
from sys import exit

def remove_from_zip(zipfname, *filenames):
    tempdir = tempfile.mkdtemp()
    try:
        tempname = os.path.join(tempdir, 'new.zip')
        with zipfile.ZipFile(zipfname, 'r') as zipread:
            with zipfile.ZipFile(tempname, 'w') as zipwrite:
                for item in zipread.infolist():
                    if item.filename not in filenames:
                        data = zipread.read(item.filename)
                        zipwrite.writestr(item, data)
        shutil.move(tempname, zipfname)
    finally:
        shutil.rmtree(tempdir)

def cargill(sn,end):
    
    print('Modulo cargill executado! em ', sn,'\n')

    if sn[-8:] == 'S2K00384':
        list_sn = ['S1M07110', 'S1M07112']
    elif sn[-8:] == 'S2K00386':
        list_sn = ['S1M06672', 'S1M06678']


    tempdir = tempfile.mkdtemp()
    prefixes = ['Genset PS', 'Genset ST']
    with zipfile.ZipFile(end, mode = 'r') as zf :
        dfMainEng = pd.read_csv(zf.open(sn + '.csv'), encoding='utf-16le', dtype=object)
        dfMainEng = dfMainEng.drop(columns=list(dfMainEng.filter(regex = 'PS Engine')))
        dfMainEng = dfMainEng.drop(columns=list(dfMainEng.filter(regex = 'ST Engine')))

    remove_from_zip(end, sn + '.csv')

    for i, prefix in enumerate(prefixes):
        listColEngAux = list(dfMainEng.filter(regex = prefix))
        listColEngAux.insert(0,'Sample Time')
        dfEngAux = dfMainEng.loc[:,listColEngAux]
        listColEngAux.remove('Sample Time')
        dfMainEng = dfMainEng.drop(columns=listColEngAux)
        
        #Fix load factor para porcentagem
        loadCol = prefix + ' Power Factor'
        if loadCol in dfEngAux.columns:
            dfEngAux[loadCol] = pd.to_numeric(dfEngAux[loadCol], errors='ignore')
            dfEngAux[loadCol] = dfEngAux.loc[dfEngAux[loadCol] <= 1.2, loadCol]*100

        dfEngAux.to_csv(tempdir + '/' + list_sn[i] + '.csv', index=False, encoding = 'utf-16le')
    
    dfMainEng.to_csv(tempdir + '/' + sn + '.csv', index=False, encoding = 'utf-16le')
    list_sn.insert(0,sn)

    with zipfile.ZipFile(end, mode = 'a') as zf :
        for nome in list_sn:
            dirr = tempdir + '/' + nome + '.csv'
            zf.write(dirr, os.path.basename(dirr))

    shutil.rmtree(tempdir)
    zf = zipfile.ZipFile(end, mode = 'r')
    
    return zf

if __name__ == '__main__':
    print('Você deve executar o arquivo GUI.py do conversor.')
    exit()


        

            


