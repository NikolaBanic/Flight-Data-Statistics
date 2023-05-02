# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 11:02:30 2022

@author: nikol
"""

import dask.dataframe as dd
import pandas as pd
import os
import time
from itertools import product

if not os.path.exists('data'):
    os.mkdir('data')
    
years = [2018, 2019, 2020, 2021, 2022] 

# Broj zračnih luka
Target_airports = 10

# Broj avionskih kompanija 
Target_airline = 10

file_path = 'data/short_flight_data.csv'

print(f'\nPodaci letova se učitavaju za godine: {years}')
print('Svaki file sa pojedinom godinom leta ima oko 2 gb podataka')
print(f'\nAnaliza kašnjenja se računa za {Target_airports} najprometnijih različitih avionskih luka')
print(f'Analiza kašnjenja se računa za {Target_airline} najprometnijih avionskih kompanija')
print(f'\nUkoliko se žele promjeniti parametri učitavanja podatak potrebno je promjeniti vrijednosti (Target_airports) i (Target_airline) i izbirsati file {file_path}')
print('Ukoliko se promijene parametri na veće brojeve data frame će biti značajno veći i analiza će biti sporija \n')

if not os.path.exists(file_path):
    Stime_1 = time.time()
    # Učitaj csv. file-ove i spoji ih u jedan veliki dask data frame
    df = []
    for i in range(len(years)):
        df.append(dd.read_csv(os.path.join('input_data', f'Combined_Flights_{years[i]}.csv')))

    df = dd.multi.concat(dfs = df[:], axis = 0)
    
    # Izaci kolone sa nepotrebnim podacima
    df = df.drop(df.loc[:, 'Year':'DivAirportLandings'].columns, axis = 1)

    # Izbaci podatke ukoliko je let otkazan ili preusmjeren
    df = df[(df['Cancelled'] == False) & (df['Diverted'] == False)]
    df = df.drop(df.loc[:, 'Cancelled':'DepTime'].columns, axis = 1)
    
    nAC = pd.Series(df['Airline'])
    # Broj ukupnih letova svake avionske kompanije
    nfAC = nAC.value_counts()
    print(f'Broj letova svake avionske kompanije:\n{nfAC}')
    
    AC_info = pd.DataFrame({'Airline':nfAC.index, 'Nflights':nfAC.values})
    AC_info.to_csv('data/airline_companies.csv')
    
    Target_airlines = nfAC[0:Target_airline]
    print(f'Odabrane avionske kompanije i njihov broj letova:\n{Target_airlines}')
    
    # Dodatno sužavanje data frema samo na odabrane najprometnije avionske kompanije
    df = df[(df['Airline'].isin(Target_airlines.index))]
    
    polasci = pd.Series(df['Origin'])    
    # Pobroji koliko ima polaska sa pojedine zračne luke
    P = polasci.value_counts()
    print(f'Broj različitih zračnih luka polaska: {len(P)}')

    P_info = pd.DataFrame({'Airport':P.index, 'Nflights':P.values})
    P_info.to_csv('data/airport_flights.csv')

    # Lista odabranih najprometnij zračnih luka
    Target_airports = P[0:Target_airports]
    print(f'Odabrane zračne luke i njihov broj polaska: \n{Target_airports}')
    
    # Dodatno sužavanje data frame samo na podatke sa odabranim zračnim lukama
    df = df[(df['Origin'].isin(Target_airports.index)) & (df['Dest'].isin(Target_airports.index))]
    

    df.sort_values(by = 'FlightDate', ascending = True)
    df.to_csv(file_path, single_file = True)
    Etime_1 = time.time()
    time_1 = Etime_1 - Stime_1
    time_1hms = time.strftime('%H:%M:%S', time.gmtime(time_1))
    print(f'Vrijeme sužavanja 10 gb podataka letova po odabranim kriterijima: {time_1hms}')
    
Stime_2 = time.time()
df = pd.read_csv(file_path)
df.pop('Unnamed: 0')

letovi = list(df['Origin'].unique())
letovi.sort()
# Sve moguće kombinacije letova, postoje duplikati  
letovi_comb = list(set(product(letovi, letovi)))

# Kombinacije letovem, bez duplikata
flight_comb = []
for i in range(len(letovi_comb)):
    if letovi_comb[i][0] != letovi_comb[i][1]:
        flight_comb.append(letovi_comb[i])
        
print(f'Od postojećih {Target_airports} zračnih luka postoje {len(flight_comb)} kombinacija letova ')

airlines = df['Airline'].unique()
airlines_copy = df['Airline'].unique()
# Lista svih zračnih kompanija
ac_nof = pd.Series(df['Airline']).value_counts()
print(f'Broj avionskih kompanija: {len(airlines)} \nBroj letova pojedine kompanije: \n{ac_nof}')

path_airline_delay = 'data/airline_delay.csv'
if os.path.exists(path_airline_delay):
    os.remove(path_airline_delay)

# Koliko koj let kasni, ako je uranio je vrijednost 0
delay_AC_1 = []
# Koliko koj let kasni, ako je uranio vrijednost je negativna
delay_AC_2 = []
for i in range(len(airlines)):
    delay_AC_1.append(df.loc[(df['Airline'] == airlines[i])]['DepDelayMinutes'].mean())
    delay_AC_2.append(df.loc[(df['Airline'] == airlines[i])]['DepDelay'].mean())
    # print(f'{(i/len(airlines))*100 + len(airlines)}% analize kašnjena je izvršeno')
    
AC_podaci_1 = {'Airline company': airlines, 'Average delay true':delay_AC_1} 
AC_podaci_2 = {'Airline company copy': airlines, 'Average delay':delay_AC_2} 
AC_podaci_3 = {'Airline company copy': airlines, 'Number of flights':ac_nof}
# Data frame sa prosječnim kašnjenjem svih avionskih kompanija

df_AC_1 = pd.DataFrame(AC_podaci_1).sort_values(by = 'Airline company', ascending = False)
df_AC_1 = df_AC_1.reset_index(drop = True)
df_AC_2 = pd.DataFrame(AC_podaci_2).sort_values(by = 'Airline company copy', ascending = False)
df_AC_2 = df_AC_2.reset_index(drop = True)
df_AC_3 = pd.DataFrame(AC_podaci_3).sort_values(by = 'Airline company copy', ascending = False)
df_AC_3 = df_AC_3.reset_index(drop = True)
frames = [df_AC_1, df_AC_2, df_AC_3]
AC_delay = pd.concat(frames, axis = 1)
AC_delay = AC_delay.sort_values(by = 'Average delay true', ascending = False)
AC_delay = AC_delay.reset_index(drop = True)
AC_delay.pop('Airline company copy')
AC_delay.to_csv(path_airline_delay)
print(f'\nKoliko koja kompanija kasni u prosjeku, prikazano u minutama: \n{AC_delay}\n')

# Vrti svaku kombinaciju leta za odabrane avionske kompanije
delay_true = []
delay = []
import math
for i in range(len(airlines)):
    for j in range(len(flight_comb)):
        
        delay_true.append([df.loc[(df['Airline'] == airlines[i]) &
                              (df['Origin'] == flight_comb[j][0]) &
                              (df['Dest'] == flight_comb[j][1])]['DepDelayMinutes'].mean(),
                               airlines[i], flight_comb[j][0], flight_comb[j][1]])
        delay.append([df.loc[(df['Airline'] == airlines[i]) &
                              (df['Origin'] == flight_comb[j][0]) &
                              (df['Dest'] == flight_comb[j][1])]['DepDelay'].mean(),
                               airlines[i], flight_comb[j][0], flight_comb[j][1]])

    # print(f'{(i/len(airlines))*100 + len(airlines)}% analize kašnjena za kombinacije letova pojedine avionske kompanije je izvršeno')
 
# Izbacuje nepostojeće opcije leta
delay_true = [i for i in delay_true if not any(isinstance(n, float) and math.isnan(n) for n in i)]
delay = [i for i in delay if not any(isinstance(n, float) and math.isnan(n) for n in i)]
   
print(f'\nOd {len(airlines)*len(flight_comb)} mogućih opcija za let avionske kompanije nemaju opcije za njih  \
      {(len(flight_comb)*len(airlines))-len(delay)}')

# Poredaj zakašnjenja leta po zračnoj luci polaska i dolaska
delay = sorted(delay, key = lambda x: (x[2],x[-1], x[0]))
delay_true = sorted(delay_true, key = lambda x: (x[2],x[-1], x[0]))

path_delay = 'data/delay.txt'
path_delay_true = 'data/delay_true.txt'
if os.path.exists(path_delay):
    os.remove(path_delay)
if os.path.exists(path_delay_true):
    os.remove(path_delay_true)

with open(path_delay, 'w') as fp:
    fp.write('\n'.join('{} {} {} {}'.format(x[1],x[2],x[3],x[0]) for x in delay))
    
with open(path_delay_true, 'w') as fpp:
    fpp.write('\n'.join('{} {} {} {}'.format(x[1],x[2],x[3],x[0]) for x in delay_true))
    
print('Podaci su spremljeni, pogledajte folder data/')
n = 20
print('\nKoliko koji zračni let kasni ovisno o kompaniji, zračnoj luci polaska i dolaska, prikazano u minutama:')
print(f'Prikazuje se samo {n} od {len(delay_true)} kombinacija letova, za ostale pogledajte folder data/')
print('Kašnjenje leta u minutama; Ime avionske kompanije; zračna luka polaska; zračna luka dolaska')
for i in range(n):
    print(f'{delay_true[i]}')

Etime_2 = time.time()
time_2 = Etime_2 - Stime_2
time_2hms = time.strftime('%H:%M:%S', time.gmtime(time_2))
# time_3 = time_1 + time_2
# time_3mhs = time.strftime('%H:%M:%S', time.gmtime(time_3))
print(f'Vrijeme detaljnog obrađivanja odabrane skupine podataka o letovima: {time_2hms}')
# print(f' Ukupno vrijeme obrade podatak: {time_3mhs}')

