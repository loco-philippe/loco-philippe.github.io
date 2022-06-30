# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 11:36:22 2022

@author: philippe@loco-labs.io

Objet : Test opendata des données IRVE
Points identifiés :
    - capacité de l'objet Ilist à traiter des volumes de données importants (1 million de données indexées)
    - temps de réponse linéaires / nombre de lignes (proportionnels sur les opérations de création)
    - temps de réponses d'un facteur 10 entre données simple et données ESValue
    - complémentation des données et génération Xarray opérationnel

api : https://static.data.gouv.fr/resources/fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/20220629-080611/consolidation-etalab-schema-irve-v-2.0.2-20220628.csv

Charging Point (EVSE)

"""
from collections import Counter
from time import time
import csv
import os
os.chdir('C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/ES')
from iindex import Iindex, util
from iindexset import Iindexset

chemin = 'C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/validation/irve/'

file = chemin + 'consolidation-etalab-schema-irve-v-2.0.2-20220606-propre.csv'

print('file size : ', os.stat(file).st_size)


# In[2]:


t0 = time()
with open(file, newline='', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    names = next(reader)
    data = []
    for row in reader: data.append(row)
data2 = util.list(list(zip(*data)))
print('data', time()-t0)


# In[ ]:


t0=time()
idxs = Iindexset.Iext(data2, names, fast=True)
print('idxs (len, lenlidx, sumcodec) : ', len(idxs), len(idxs.idxlen), sum(idxs.idxlen), time()-t0)
t0=time()
fullsize = len(idxs.to_obj(encoded=True, fullcodec=True))
print('fullsize', fullsize, time()-t0)
t0=time()
minsize = len(idxs.to_obj(encoded=True, defaultcodec=True, fullcodec=True))
print('minsize', minsize, time()-t0)


# In[ ]:


t0=time()
defaultsize = len(idxs.to_obj(encoded=True, defaultcodec=True))
print('defaultsize', defaultsize, time()-t0)
print('indicator default : ', idxs.indicator(fullsize, defaultsize))
t0=time()
infos = idxs.coupling(fast=True)
print('coupling', time()-t0)
t0=time()
optimizesize = len(idxs.to_obj(indexinfos=infos, encoded=True))
print('optimizesize ', optimizesize, time()-t0)
print('indicator optimize : ', idxs.indicator(fullsize, optimizesize))
t0=time()
js = idxs.to_obj(indexinfos=infos, encoded=True, encode_format='cbor')
cborsize = len(js)
print('cborsize', cborsize, time()-t0)
print('indicator cbor : ', idxs.indicator(fullsize, cborsize))


# In[ ]:


t0=time()
idxs2 = Iindexset.from_obj(js)
print('fromcbor', len(idxs2), time()-t0)
t0=time()
verif = idxs2 == idxs
print('controle égalité :', verif, time()-t0)


# In[ ]:


print('Couplage entre ', idxs[13].name, ' et ', idxs[9].name, ' : ', idxs[13].couplinginfos(idxs[9])['typecoupl'])
infosdefault = idxs[9].couplinginfos(idxs[13], default=True)
print('Ecart : ', infosdefault['disttomin'], 'positions sur ', infosdefault['distmin']) # moins de 1%
nom_station = idxs[9].tostdcodec(full=False)
coordonneesXY = idxs[13].tostdcodec(full=False) 
coordonneesXY.coupling(nom_station)
c = Counter(coordonneesXY.codec).most_common(5)
print('les 5 positions avec le plus de stations: ', c)
print('liste des stations associées à la position', c[0][0], ' : ', 
      [nom_station[i] for i in coordonneesXY.recordfromvalue(c[0][0])])

'''
import os
os.chdir('C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/ES')
from collections import Counter
from ilist import Ilist, identity
from ESObservation import Observation
from time import time
import csv
from ESValue import LocationValue, DatationValue, PropertyValue, ESValue
from test_observation import _envoi_mongo_python
import datetime
import pandas as pd
from iindex import Iindex, util
from iindexset import Iindexset

chemin = 'C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/validation/irve/'

file = chemin + 'consolidation-etalab-schema-irve-v-2.0.2-20220606-propre.csv'
typef = 'ES'
cmax=100000
ESval=False

#%% analyse fichier CSV
print('file size : ', os.stat(file).st_size)
c = 0

t0 = time()
with open(file, newline='', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    names = next(reader)
    data = []
    for row in reader: data.append(row)
data2 = util.list(list(zip(*data)))
print('data', time()-t0)
t0=time()
idxs = Iindexset.Iext(data2, names, fast=True)
print('idxs (len, lenlidx, sumcodec) : ', len(idxs), len(idxs.idxlen), sum(idxs.idxlen), time()-t0)
t0=time()
js = idxs.to_obj(encoded=True, fullcodec=True)
fullsize = len(js)
print('full', time()-t0, fullsize)
t0=time()
sizedef = len(idxs.to_obj(encoded=True, defaultcodec=True))
print('default', time()-t0, sizedef)
print('indic def : ', idxs.indicator(fullsize, sizedef))
t0=time()
infos = idxs.coupling(fast=True)
print('coupling', time()-t0)
#print(infos)
t0=time()
js = idxs.to_obj(indexinfos=infos)
print('list ', time()-t0, len(js))
t0=time()
sizecoup = len(idxs.to_obj(indexinfos=infos, encoded=True))
print('coup ', time()-t0, sizecoup)
print('indic coup : ', idxs.indicator(fullsize, sizecoup))
t0=time()
js = idxs.to_obj(indexinfos=infos, encoded=True, defaultcodec=True, fullcodec=True)
print('codec', time()-t0, len(js))
minsize = len(js)
t0=time()
js = idxs.to_obj(indexinfos=infos, encoded=True, encode_format='cbor')
sizecbor = len(js)
print('cbor', time()-t0, sizecbor)
print('indic cbor : ', idxs.indicator(fullsize, sizecbor))
t0=time()
idxs2 = Iindexset.from_obj(js)
print('fromcbor', time()-t0, len(idxs2))
t0=time()
verif = idxs2 == idxs
print('controle égalité :', verif, time()-t0)
#print(data[0:2])
#print(len(data))

#%% almost derived
print(idxs[9].couplinginfos(idxs[13], default=True)) # Ecart : 44 sur 4503 valeurs (1%)
nom_station = idxs[9].tostdcodec(full=False)
coordXY = idxs[13].tostdcodec(full=False) 
coordXY.coupling(nom_station)
c = Counter(coordXY.codec).most_common(5) # 5 stations le plus en écart
print(c)
print(c[0][0], [nom_station[i] for i in coordXY.recordfromvalue(c[0][0])])

#%% infos 
'''
'''
data 0.21158075332641602
idxs 2.502021551132202
fullinit 9615178
coupling 39.87991523742676
full 15.532855033874512 9615178
list  7.657615900039673 49
coupling  7.904985427856445 2491520
indic coup :  {'unicity level': 0.11614082235958076, 'object lightness': 0.16177104826934124, 'gain': 0.7408763519510507}
default 2.2779595851898193 3720521
indic def :  {'unicity level': 0.11614082235958076, 'object lightness': 0.3063855454709098, 'gain': 0.6130575013795897}
codec 2.0157527923583984 1388222
cbor 7.235912561416626 1715121
indic cbor :  {'unicity level': 0.11614082235958076, 'object lightness': 0.07041347328543106, 'gain': 0.8216235830475526}fromcbor 2.6185598373413086 11163
controle égalité : True 0.21908783912658691'''

"""
#%% test pandas
t=time()
dt = {"dep":"string","sexe":"int","hosp":"int","rea":"int","rad":"int","dc":"int"}
df = pd.read_csv(file, sep=';', dtype=dt)
tf=time()
print('pandas : ', tf-t)


#%% analyse fichier CSV
print('départ : ', t0)
with open(file, newline='') as f:
    reader = csv.reader(f, delimiter=';')
    names = next(reader)
    prp = [PropertyValue.Simple(nam, name='') for nam in names]
    iprp = [3,4,8,9]
    data = [[], [], [], []]
    res = []
    c=0
    if ESval :
        for row in reader:
            c +=1
            loc = LocationValue('dpt' + str(row[0]))
            dat = DatationValue(row[2])
            for i in iprp:
                data[0].append(dat)
                data[1].append(loc)
                data[2].append(row[1])
                data[3].append(prp[i])
                res.append(ResultValue(Ilist._cast(row[i], 'int')))
            if c == cmax : break
    else :
        for row in reader:
            c +=1
            dat = Ilist._cast(row[2], 'datetime')
            loc = row[0]
            for i in iprp:
                data[0].append(dat)
                data[1].append(loc)
                data[2].append(row[1])
                data[3].append(i)
                res.append(Ilist._cast(row[i], 'int'))
            if c == cmax : break
t1 = time()
print('délai data: ', t1 - t0) # 1s pour c= 2000, 10s pour 20 000, 93s (1,5 mn) pour 230 000 (soit 1 142 000 lignes)
print('nombre de lignes : ', len(res))

#%% création Ilist
il = Ilist.Iext(res, data, 'result', ['datation', 'location', 'sexe', 'property'], fast=True)
t2 = time()
print('délai il: ', t2 - t1) # 4s pour c= 2000, 37s pour 20 000, 360s (6 mn) pour 230 000 (soit 1 142 000 lignes)
# total : 1 mn pour 100 000 enregistrements Ilist

#%% stockage Ilist
#print(il.to_obj(encoded=False,  bjson_bson=True, fast=True))
il.to_file(chemin + 'il_numeric_bson_bidon.il', bjson_bson=True, fast=True)
il.to_file(chemin + 'il_numeric_json_bidon.il', bjson_bson=False, fast=True)
ilf=il.full()
ilf.to_file(chemin + 'ilf_numeric_bson_bidon.il', bjson_bson=True, fast=True)
ilf.to_file(chemin + 'ilf_numeric_json_bidon.il', bjson_bson=False, fast=True)
t2b = time()
print('délai file: ', t2b - t2)
'''
if ESval: setidxf = [[idx for idx in il.setidx[0] if idx.vSimple().year == 2020], il.setidx[1], il.setidx[2], il.setidx[3]]
else: setidxf = [[idx for idx in il.setidx[0] if idx.year == 2020], il.setidx[1], il.setidx[2], il.setidx[3]]
il2020 = il.setfilter(setidxf, inplace=False, index=False, fast=True)
t4 = time()
print('délai il2020: ', t4 - t2) # 3s pour c= 2000, 30s pour 20 000, 120s (2 mn) pour 440 725 sur 1 142 000 lignes)

if ESval: setidxf = [[idx for idx in il.setidx[0] if idx.vSimple().year == 2021], il.setidx[1], il.setidx[2], il.setidx[3]]
else: setidxf = [[idx for idx in il.setidx[0] if idx.year == 2021], il.setidx[1], il.setidx[2], il.setidx[3]]
il2021 = il.setfilter(setidxf, inplace=False, index=False, fast=True)
t5 = time()
print('délai il2021: ', t5 - t4)  # 3s pour c= 2000, 30s pour 20 000, 140s (2,3 mn) pour 440 725 sur 1 142 000 lignes)

if ESval: setidxf = [[idx for idx in il.setidx[0] if idx.vSimple().year == 2022], il.setidx[1], il.setidx[2], il.setidx[3]]
else: setidxf = [[idx for idx in il.setidx[0] if idx.year == 2022], il.setidx[1], il.setidx[2], il.setidx[3]]
il2022 = il.setfilter(setidxf, inplace=False, index=False, fast=True)
t6 = time()
print('délai il2022: ', t6 - t5) # 3s pour c= 2000, 30s pour 20 000, 39s  pour 144 875 sur 1 142 000 lignes)

#%% stockage
js = il.to_bytes()
_envoi_mongo_python(il2020.to_bytes(encoded=False))
t7 = time()
print('délai to_bytes: ', t7 - t6) # 144k pour c=2000, 1,5 Mo pour c=20000, 20,6 Mo au total (9 Mo pour 2021)
print("size : ", len(js))
if ESval: typef = 'ES'
else : typef = 'int'
il2020.to_file(chemin + 'il2020' + typef + str(cmax) + '.il')
il2021.to_file(chemin + 'il2021' + typef + str(cmax) + '.il')
il2022.to_file(chemin + 'il2022' + typef + str(cmax) + '.il')
t8 = time()
print('délai stockage: ', t8 - t7) #

il2020 = Ilist.from_file(chemin + 'il2020' + typef + str(cmax) + '.il')
il2021 = Ilist.from_file(chemin + 'il2021' + typef + str(cmax) + '.il')
il2022 = Ilist.from_file(chemin + 'il2022' + typef + str(cmax) + '.il')
if ESval:
    il2020.setidx[0] = DatationValue.cast(il2020.setidx[0])
    il2020.setidx[1] = LocationValue.cast(il2020.setidx[1])
    il2020.setidx[3] = PropertyValue.cast(il2020.setidx[3])
    il2020.extval    = ResultValue.  cast(il2020.extval)
    il2021.setidx[0] = DatationValue.cast(il2021.setidx[0])
    il2021.setidx[1] = LocationValue.cast(il2021.setidx[1])
    il2021.setidx[3] = PropertyValue.cast(il2021.setidx[3])
    il2021.extval    = ResultValue.  cast(il2021.extval)
    il2022.setidx[0] = DatationValue.cast(il2022.setidx[0])
    il2022.setidx[1] = LocationValue.cast(il2022.setidx[1])
    il2022.setidx[3] = PropertyValue.cast(il2022.setidx[3])
    il2022.extval    = ResultValue.  cast(il2022.extval)
t9 = time()
print('délai chargement: ', t9 - t8) # 4.8s

#%% export
if ESval: fillvalue = ResultValue()
else : fillvalue = 0
ilf2020=il2020.full(fillvalue=fillvalue)
t10 = time()
print('full 2020 : ', t10 - t9) # 39s pour ajouter 1156 res à 352 600 res, 70s pour ESValue

if ESval: np2020=ilf2020.to_numpy(func=ESValue.vSimple, fast=True)
else: np2020=ilf2020.to_numpy(fast=True)
t11 = time()
print('numpy 2020 : ', t11 - t10) # 39s pour ajouter 1156 res à 352 600 res

if ESval:
    function = [ESValue.vSimple, ESValue.vName, identity, ESValue.vSimple]
    ilx=il2020.to_xarray(ifunc=function)
t12 = time()
print('xarray 2020 : ', t12 - t11) # 39s pour ajouter 1156 res à 352 600 res

if ESval:
    res = ilx.loc["2020-03-23", "dpt06", '1', "dc"]
t13 = time()
print('xarray 2020 loc : ', t13 - t12) #

if ESval:
    sort=[i for i in range(len(il2020)) if il2020.iidx[2][i] ==0]
    il2020.reorder(sort)
    il2020.swapindex([0, 1, 3])
    ob2020 = Observation.Ilist(il2020)
    ob2020.to_file(chemin + 'ob2020' + typef + str(cmax) + '.ob', bjson_bson=True)
    ob20202=Observation.from_file(chemin + 'ob2020' + typef + str(cmax) + '.ob')
t14 = time()
print('ob 2020 write-read : ', t14 - t13)

if ESval:
    obx2020 = ob2020.to_xarray(numeric=True)
t15 = time()
print('ob xarray 2020 : ', t15 - t14)

if ESval:
    ob2020f=ob2020.filter(location={'isName' : 'dpt7.'}, 
                          datation={'within':DatationValue([datetime.datetime(2020,3,19),
                                                            datetime.datetime(2020,3,22)])})
    ob2020f.plot()
t16 = time()
print('ob filter plot : ', t16 - t15)

#%% synthèse
res = [t11-t0, t1-t0, t2-t1, t4-t2, t5-t4, t6-t5, t7-t6, t8-t7, t9-t8, t10-t9,
       t11-t10, t12-t11, t13-t12, t14-t13, t15-t14, t16-t15]
print(res)

print('délai total(mn) : ', (time() - t0)/60,  time() - t0)
'''

"""