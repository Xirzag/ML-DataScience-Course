import numpy as np
import pandas as pd
import glob
import json
from datetime import datetime


def process_joined_ks(ks, file_name):
    # quitamos columnas duplicadas por el merge
    ks = ks.drop(columns=['blurb_l', 'category_l',
       'country_l', 'created_at_l', 'creator_l', 'currency_l', 'currency_symbol_l',
       'currency_trailing_code_l', 'current_currency_l', 'deadline_l',
       'disable_communication_l', 'friends_l', 'fx_rate_l', 'goal_l',
       'is_backing_l', 'is_starrable_l', 'is_starred_l', 'launched_at_l', 'location_l',
       'name_l', 'permissions_l', 'photo_l', 'profile_l', 'slug_l',
       'source_url_l', 'spotlight_l', 'staff_pick_l', 'state_l',
       'state_changed_at_l', 'static_usd_rate_l', 'urls_l', 'converted_pledged_amount_l', 'usd_type_l', 0])
    # las columnas que cambian las he llamado start_(nombre de la columna)
    ks = ks.rename(index=str, columns={'usd_pledged_l': 'start_usd_pledged_amount',
                                       'backers_count_l': 'start_backers_count',
                                       'pledged_l': 'start_pledged'})

    # obtenemos nombres del campo categoría
    categories_pos = []
    categories_name = []
    categories_slugs = []
    categories_subslugs = []
    for category in ks['category']:
        category_json = json.loads(category)

        categories_name.append(category_json['name'])
        try:
            categories_pos.append(category_json['position'])
        except KeyError:
            categories_pos.append(15) # por poner uno, aunque creo que siempre hay

        try:
            parts = (category_json['slug'] or category_json['slugs']).split('/')
            categories_slugs.append(parts[0])
            if len(parts) > 1:
                categories_subslugs.append(parts[1])
            else:
                categories_subslugs.append('None')
        except KeyError:
            categories_slugs.append('None')  # en ocasiones no hay
            categories_subslugs.append('None')

    # nuevas columnas
    ks['file_name'] = [file_name] * ks.shape[0]
    ks['category_name'] = pd.Series(categories_name, index=ks.index)
    ks['category_pos'] = pd.Series(categories_pos, index=ks.index)
    # Slugs tiene la categoría principal
    ks['category_slugs'] = pd.Series(categories_slugs, index=ks.index)
    ks['category_subslugs'] = pd.Series(categories_subslugs, index=ks.index)
    # converted_goal y converted_pledge_amount parece que son las que deberíamos usar
    # Es el valor en $
    ks['usd_goal'] = ks['static_usd_rate'] * ks['goal']

    ks['duration'] = ks['deadline'] - ks['launched_at']
    ks['completed_time'] = (ks['timestamp_l'] - ks['launched_at']) / ks['duration']

    return ks


#Orden de las columnas que me las desordenan
column_order = None

# carga los nombres de los archivos
file_names = glob.glob('C:/data/*/*.csv')
files_amount = len(file_names)

print('Files:', files_amount)


processed_rows = 0
saved_rows = 0


# va a guardar los kickstarter que esten activos, pero no haya encontrado aun el registro de como terminaron
live_rows = pd.Series([])

# por cada archivo
for i, file_name in enumerate(file_names):
    print('Adding', i, '/', files_amount, ' ', file_name)

    df = pd.read_csv(file_name)
    processed_rows += df.shape[0] #para saber cuantas hemos leido

    # hay un par de datasets que no tienen la columna source_url
    if ~df.columns.contains('source_url'):
        df['source_url'] = ['None'] * df.shape[0] # le pongo una que se llame None

    # hay un par de datasets que no tienen la columna spotlight
    if ~df.columns.contains('spotlight'):
        df['source_url'] = [''] * df.shape[0]  # Lo guardo como que nunca estuvo en spotlight

    # mascara para filtrar cual estan activos
    live = df['state'] == 'live'

    df['timestamp'] = [datetime.strptime(file_name[20:30], '%Y-%m-%d').timestamp()] * df.shape[0]

    # comprobamos si hay un kickstarter finalizado que este en la lista de activos
    if i is not 0: # solo después de la primera vuelta

        # juntamos los kickstarter activos con los finalizados que acabamos de encontrar
        ks = pd.merge(df[~live], live_rows, how='inner', on='id', suffixes=('', '_l'), sort=False)
        # los retiramos de la lista de kickstarter activos
        live_rows = live_rows[~live_rows['id'].isin(ks['id'])]

        # procesamos el nuevo dataframe, para quitar columnas duplicadas, añadir otras...
        processed_ks = process_joined_ks(ks, file_name)

        if i is 1: # guardamos el orden de las columnas
            column_order = processed_ks.columns
        else: # para los siguientes datasets ordenamos las columnas (por si vienen desordenadas)
            processed_ks = processed_ks[column_order]

        # añadimos al archivo
        processed_ks.to_csv('kickstarter_joined_full5.csv', header=i is 1, mode='a', index=False)
        saved_rows += processed_ks.shape[0]


    live_samples = df[live] # .sample(frac=.4) # Con todos los registros va bien

    # guardamos los kickstarter activos
    live_rows = pd.concat([live_rows, live_samples], ignore_index=True, sort=False)



print('Lineas leidas:', processed_rows)
print('Lineas guardadas:', saved_rows)


