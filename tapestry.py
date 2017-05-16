import sqlalchemy as sa
import pandas as pd

engine = sa.create_engine("crate://db.world.io:4200")

BB = """POLYGON((-71.1209522 42.4025742,
            -71.1011757 42.4025742,
            -71.1011757 42.3958145,
            -71.1209522 42.3958145,
            -71.1209522 42.4025742))"""

#######blocks in BB
q = """select count(*) from world
    where ingestid = '25cb2427c0a5710a983e2f3a1f484bc6'
     and match(shape, '{}') using intersects""".format(BB)
z = pd.read_sql(q, engine)

q = """select shape, data['t_geoid10'] geoid10  from world
    where ingestid = '25cb2427c0a5710a983e2f3a1f484bc6'
     and match(shape, '{}') using intersects limit {}""".format(BB, z['count(*)'][0])
blocks = pd.read_sql(q, engine)

from shapely.geometry import shape
geo = [shape(i) for i in blocks['shape']]
del blocks['shape']

import geopandas as gpd
blocks.gpd = gpd.GeoDataFrame(geometry=geo, data=blocks, crs={'init': 'epsg:4326'})

###### Housing Units
q = """select count(*)  from world
        where ingestid = '139fa87c22976bbd156da6dd9ab672ec'
        and point is not null
        and match(shape, '{}')using intersects""".format(BB)
z = pd.read_sql(q, engine)

q = """select shape, data['t_propertygroup'] propertygroup, data['t_propertytype'] propertytype
        from world where ingestid = '139fa87c22976bbd156da6dd9ab672ec'
        and point is not null
        and match(shape, '{}')using intersects limit {}""".format(BB, z['count(*)'][0])
houses = pd.read_sql(q,engine)

geo = [shape(i) for i in houses['shape']]
del houses['shape']
houses.gpd = gpd.GeoDataFrame(geometry=geo, data=houses, crs={'init': 'epsg:4326'})

h = ['Single Family Residence','Condominium', 'Triplex (3 units, any combination)',
                               'Multi-Family Dwellings (Generic, any combination)',
                               'Duplex (2 units, any combination)', 'Quadplex (4 Units, Any Combination)',
                               'Apartment house (5+ units)', 'Quadruplex (4 units, any combination)']

houses.gpd = houses.gpd[houses.gpd['propertytype'].isin(h)]
houses.gpd['type'] = 'residential'
del houses.gpd['propertygroup']
del houses.gpd['propertytype']

###### Biz dataselect * from sys.jobs limit 100;

q = """select count(*) from world
        where ingestid = 'f67b619608a7ae6027824d27c330ffc0'
        and match(shape, '{}')using within""".format(BB)
z = pd.read_sql(q, engine)

q = """
select a.data['i_pid'] pid, a.shape, a.data['i_is_restaurant'] is_restaurant, cast(b.data['i_sic'] as string) sic
from world a, world b
where a.ingestid = 'f67b619608a7ae6027824d27c330ffc0'
and b.ingestid = 'ef2fee4be9259d0b0d3eff2cabc86fbc'
and match(a.shape, '{}')using within
and a.data['i_pid'] = b.data['i_pid']
limit {}
""".format(BB,z['count(*)'][0]*10)

biz = pd.read_sql(q, engine)
geo = [shape(i) for i in biz['shape']]
del biz['shape']
biz.gpd = gpd.GeoDataFrame(geometry=geo, data=biz, crs={'init': 'epsg:4326'}).copy()
biz.gpd.to_csv('bizInBB_w_sic.tsv', index=False, sep='\t')
print('done')

from pandasql import *
del biz['geometry']
biz.gpd['type'] = ''

pysqldf = lambda q: sqldf(q, globals())

q = """
select pid from biz
where sic like '81%'
or sic like '87%'
or sic like '731%'
or sic like '737%';
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='professional')

q = """
select pid from biz
where sic like '53%'
or sic like '55%'
or sic like '56%'
or sic like '57%'
or sic like '59%';
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='retail')

q = """
select pid from biz
where sic like '701%';
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='hotel')

q = """
select pid from biz
where sic like '793%'
or sic like '84%'
or sic like '783%'
or sic like '7993%';
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='culture_entertainment')

q = """
select pid from biz
where sic like '91%'
or sic like '921%';
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='government')

q = """
select pid from biz
where is_restaurant =1;
"""
k = pysqldf(q)
key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
biz.gpd.set_value(index=key, col='type', value='restaurant')

biz.gpd = biz.gpd.loc[biz.gpd['type'] != '']
biz.gpd = biz.gpd.reset_index(drop=True)
del biz.gpd['pid']
del biz.gpd['is_restaurant']
del biz.gpd['sic']

##########  All points together

points = houses.gpd.append(biz.gpd, ignore_index=True)
points = points.reset_index(drop=True)

base = blocks.gpd.plot(color='white')
points.plot(ax=base, marker='o', color='red', markersize=5)

###### Intersection

from rtree import index
idx = index.Index()
for index, row in blocks.gpd.iterrows():
    idx.insert(index,row.geometry.bounds)

def seba_over(point):
    poly_idx = [i for i in idx.intersection((point.coords[0]))
                if point.within(blocks.gpd.geometry[i])]
    return(poly_idx[0])

import timeit
from multiprocessing import Pool
start_time = timeit.default_timer()
if __name__ == '__main__':
    with Pool(8) as p:
        poly_ids = p.map(seba_over, points.geometry)
elapsed = timeit.default_timer() - start_time
print(elapsed)
bid = blocks.gpd.geoid10[poly_ids]
points['blockID'] = list(bid)

points['type'].value_counts()
x = points[['type', 'blockID']]
x['values'] = 1
pt = pd.pivot_table(data=x, index = 'type', columns='blockID', aggfunc=sum, fill_value=0,
                    dropna=False, values='values').astype(float)
def f(x):
    return(x/x.max())

pt = pt.apply(f, axis=1)
pt = pt.apply(f, axis=0)

from numpy import argmax
labels = pd.DataFrame(data={'type':pt.apply(argmax,0)})
labels['geoid10'] = labels.index
blocks.gpd = blocks.gpd.merge(labels, on='geoid10')
blocks.gpd.to_file('TapestryTest.geojson')