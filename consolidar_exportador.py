#python36

import pandas as pd
import glob
import os
import os.path

print('Paso 1 de 6 - Borrando consolidado anterior')
if os.path.isfile("consolidado_exportador_de_datos.csv"):
    os.remove("consolidado_exportador_de_datos.csv")

archivoscsv=glob.glob("*.csv")

print('Paso 2 de 6 - Leyendo archivos')

archivos_del_exportador=[]
for i in archivoscsv:
	archivos_del_exportador.append(pd.read_csv(i,encoding='latin1', header=None, low_memory=False))

#crear primera linea
print('Paso 3 de 6 - Creando encabezados, eliminando filas inncesarias')
 #primeras columnas
for i in range(0, len(archivoscsv)):
    encabezado=['01_PROCESO','02_FOLIO','03_AÑO PAGO','04_MES PAGO','05_AÑO DEVENGO','06_MES DEVENGO',
        '07_RUT-DV','08_CORR','09_CORR. PAGO','10_NOMBRE','11_ESTAB']

    archivos_del_exportador[i].replace(['INDICADOR','TOTALES','DESCUENTO'],['A_INDICADOR','B_TOTALES','Z_DESCUENTO'],inplace=True)
    #concatenar las siguientes columnas
    for j in range(11,archivos_del_exportador[i].shape[1]):
        encabezado.append(str(archivos_del_exportador[i].iloc[0][j])+"_"+str(archivos_del_exportador[i].iloc[1][j])+"_"+str(archivos_del_exportador[i].iloc[2][j]))

    #dejar la linea en primer lugar
    archivos_del_exportador[i].loc[-1] = encabezado  # adding a row
    archivos_del_exportador[i].index = archivos_del_exportador[i].index + 1  # shifting index
    archivos_del_exportador[i]= archivos_del_exportador[i].sort_index()  # sorting by index

    new_header = archivos_del_exportador[i].iloc[0] #grab the first row for the header
    archivos_del_exportador[i] = archivos_del_exportador[i][1:] #take the data less the header row
    archivos_del_exportador[i].columns = new_header #set the header row as the df header

    #eliminando las tres primeras filas
    archivos_del_exportador[i].drop(archivos_del_exportador[i].index[[0,1,2]],inplace=True)


print('Paso 4 de 6 - Concatenando archivos')

consolidado=pd.concat(archivos_del_exportador)
consolidado=pd.concat(archivos_del_exportador).reset_index(drop = True)


print ('Paso 5 de 6 - Grabando Archivo')
consolidado.to_csv('consolidado_exportador_de_datos.csv', encoding='latin1',index=False)

print('Paso 6 de 6 - Archivo listo')
