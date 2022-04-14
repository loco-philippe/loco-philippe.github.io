# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 10:49:19 2022

@author: philippe@loco-labs.io

api : https://data.gers.fr/explore/dataset/base-adresse-nationale/export/?disjunctive.nom_commune&refine.nom_voie=Rue+de+la+R%C3%A9publique&basemap=jawg.streets&location=9,43.68895,0.4616

objet : tests opendata sur la base adresse nationale
points identifiés :
    - lecture fichiers csv et json -> même résultat
    - mise en évidence du typage des données non défini en csv
    - mise en évidence du format json non explicite (champs json non définis)
    - nécessité de retraitement pour reconstruire les données
    - gain de taille significatif sur le stockage ( 18% sur csv, 76% sur json, 89% sur xls)
    - identification des colonnes et lignes de référence
    - indicateurs sur les données
"""

import os
os.chdir('C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/ES')
from ilist import Ilist
from time import time
from pprint import pprint
import json
import csv

chemin = 'C:/Users/a179227/OneDrive - Alliance/perso Wx/ES standard/python ESstandard/validation/base adresse/'

#%% test
def affiche(il, file, column):
    if column: sens ='colonnes'
    else: sens='lignes'
    lenidx = il.lenidx
    idxcoupled = il.idxcoupled
    idxderived = il.idxderived
    idxref = il.idxref
    idxder = il.idxder
    axesmin = il.axesmin
    pprint({sens + ' non couplées : ' : [il.idxname[i]  for i in range(lenidx) if not idxcoupled[i]],
            sens + ' non dérivées : ' : [il.idxname[i]  for i in range(lenidx) if not idxderived[i]]}, compact=True)
    if column:
        pprint({sens + ' couplées     : ' : [il.idxname[i] + ' <-> ' + il.idxname[idxref[i]]  for i in range(lenidx) if idxcoupled[i]],
                sens + ' dérivées     : ' : [il.idxname[i] + '  -> ' + il.idxname[idxder[i]]  for i in range(lenidx) if idxderived[i]]})
        pprint(il._dict(), compact = True, sort_dicts=False)
    print('axes minimaux : ', [il.idxname[i]  for i in axesmin])
    print('taille fichier : ', il.to_file(file), '\n')

    #print([il.idxname[i]  for i in range(lenidx) if not idxcoupled[i]])

#%% analyse fichier CSV
file = chemin + 'base-adresse-nationale.csv'
l = ['str' for i in range(24)]
l[2] = l[5] = l[6] = l[8] = l[21] = 'int'
l[10] = l[11] = l[12] = l[13] = 'float'
l[23] = 'coord'
il = Ilist.from_csv(file, valfirst=True, delimiter=';', dtype=l)
affiche(il, 'BAN_gers_csv.il', True)

#%% analyse fichier JSON
file = chemin + 'base-adresse-nationale.json'
with open(file, 'rb') as f: js = f.read()
dic=json.loads(js)
#name = list(dic[0]['fields'].keys()) + ['rep', 'type_position', 'cad_parcelles'] # manque beaucoups
name = ['id', 'id_fantoir', 'numero', 'rep', 'nom_voie', 'code_postal', 'code_insee',
        'nom_commune', 'code_insee_ancienne_commune', 'nom_ancienne_commune', 'x',
        'y', 'lon', 'lat', 'type_position', 'alias', 'nom_ld', 'libelle_acheminement',
        'nom_afnor', 'source_position', 'source_nom_voie', 'certification_commune',
        'cad_parcelles', 'geopoint']
#lis = Ilist._transpose([list(rec['fields'].values()) for rec in dic])  # marche pas, champs manquant
lis = Ilist._transpose([[rec['fields'][keys] if keys in rec['fields'] else ''
                         for keys in name ] for rec in dic])
extval = lis[name.index('id')]
lis.pop(name.index('id'))
name.pop(name.index('id'))
il2 = Ilist.Iext(extval=extval, extidx=lis, valname='BAN Gers', idxname=name)
affiche(il2, 'BAN_gers_json.il', True)

#%% analyse sur les lignes
lis2 = [[rec['fields'][keys] if keys in rec['fields'] else '' for keys in name ] for rec in dic]
il3 = Ilist.Iext(extidx=lis2, valname='BAN Gers transpose')
affiche(il3, 'BAN_gers_json_transp.il', False)
