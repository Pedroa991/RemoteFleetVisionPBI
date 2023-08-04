#RAGNAR v1
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

def carlao(sn,end):
    print('Modulo carlao executado! em ', sn, '\n')
    zf = zipfile.ZipFile(end, mode = 'r')
    csv = pd.read_csv(zf.open(sn + '.csv'), encoding='utf-16le', dtype=object)
    zf.close()
    listmotaux = list(csv.filter(regex = 'C4.4'))
    scquatro = csv.drop(columns = listmotaux)
    listmotaux.insert(0,'Sample Time')
    cquatro = csv.loc[:,listmotaux]
    tempdir = tempfile.mkdtemp()
    
    listnomes = [sn, 'MCA - D1K01363']
    listadf = [scquatro, cquatro]
    
# =============================================================================
#     for nome in listnomes:
#         df = pd.read_csv(tempdir + '\\' + nome + '.csv')
#         df.to_csv(tempdir + '\\' + nome + '.csv', index=False)
# =============================================================================
    remove_from_zip(end, sn + '.csv')
    zf = zipfile.ZipFile(end, mode='a')
    # zf.printdir()
    
    i=0
    for nome in listnomes:
        listadf[i].to_csv(tempdir + '/' + nome + '.csv', index=False, encoding = 'utf-16le')
        dirr = tempdir + '/' + nome + '.csv'
        zf.write(dirr, os.path.basename(dirr))
        i=i+1
    #zf.printdir()
    zf.close()
    shutil.rmtree(tempdir)
    zf = zipfile.ZipFile(end, mode = 'r')
    
    return zf
    

if __name__ == '__main__':
    print('Você deve executar o arquivo GUI.py do conversor.')
    exit()
