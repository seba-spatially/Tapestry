import sqlalchemy as sa
import pandas as pd

engine = sa.create_engine("crate://db.world.io:4200")
msa = 'Boston'
q = """select wkt['solid']
from shaped
where rowid = (select rowid from world where datasetid='metro/tiger/2014/1/tl_2014_us' and data['t_namelsad'] like '%{}%') limit 10;
""".format(msa)
z = pd.read_sql(q, engine)
BB = z["wkt['solid']"][0]


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

def bizLoc(msa=msa):
    import sqlalchemy as sa
    import pandas as pd
    engine = sa.create_engine("crate://db.world.io:4200")
    msa = 'boston'
    q = "select datasetid, metadata['table'] _table, ingestid, major, minor "\
    + "from dataset where datasetid like 'business%metro-{}' ".format(msa)\
    + "order by datasetid, major desc, minor desc"
    df = pd.read_sql(q, engine)
    df.head()
    x = df.loc[df.major == df.major.max()]
    x = x.loc[x.minor == x.minor.max()]
    x = x.reset_index(drop=True)
    out = {'ingestID':x.ingestid[0], 'table':x._table[0]}
    return(out)

bl = bizLoc('boston')
q = """select count(*) from {}
        where ingestid = '{}'""".format(bl['table'], bl['ingestID'])
z = pd.read_sql(q, engine)

q = """
select a.data['i_pid'] pid, a.shape, a.data['i_is_restaurant'] is_restaurant, a.data['t_sic'] sic
from {} a
where a.ingestid = '{}'
limit {}
""".format(bl['table'], bl['ingestID'], z['count(*)'][0]*10)

from shapely.geometry import shape
import geopandas as gpd
biz = pd.read_sql(q, engine)
geo = [shape(i) for i in biz['shape']]
del biz['shape']
biz.gpd = gpd.GeoDataFrame(geometry=geo, data=biz, crs={'init': 'epsg:4326'}).copy()
biz.gpd.to_csv('bizBoston_w_sic.tsv', index=False, sep='\t')
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

#base = blocks.gpd.plot(color='white')
#points.plot(ax=base, marker='o', color='red', markersize=5)

###### Intersection

from rtree import index
idx = index.Index()
for index, row in blocks.gpd.iterrows():
    idx.insert(index,row.geometry.bounds)

import timeit
start_time = timeit.default_timer()

def seba_over(point):
    poly_idx = [i for i in idx.intersection((point.coords[0]))
                if point.within(blocks.gpd.geometry[i])]
    if poly_idx:
        out = {'polyID':poly_idx[0]}
    else:
        out = {'polyID':-999}
    return(out)

import timeit
from multiprocessing import Pool
start_time = timeit.default_timer()
if __name__ == '__main__':
    with Pool(8) as p:
        poly_ids = p.map(seba_over, points.geometry)
elapsed = timeit.default_timer() - start_time
print(elapsed/60)
poly_ids = pd.DataFrame(poly_ids)
poly_ids.head()

points['blockID'] = poly_ids

x = points[['type','blockID']]
y = blocks.gpd[['geoid10']]
y['blockID'] = blocks.gpd.index

data = x.merge(y, on='blockID')
data['values'] = 1


pt = data.groupby(['geoid10', 'type'])['values'].sum().unstack()
pt = pt.fillna(value=0)
def f(x):
    return(x/x.max())
pt = pt.apply(f, axis=0)
pt = pt.apply(f, axis=1)
from numpy import argmax
labels = pd.DataFrame(data={'type':pt.apply(argmax,1)})
labels['geoid10'] = labels.index
blocks.gpd = blocks.gpd.merge(labels, on='geoid10', how='left')
blocks.gpd.to_file('TapestryTest2.geojson', driver='GeoJSON')
