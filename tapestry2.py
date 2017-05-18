#Due to the multiprocessing method used below  I can't wrap the whole process in a function without major surgery
#Therefore the way to generate tapestries is to define the variable "msa" with the name of the MSA we want to process
#and then run the entire script.


#Make sure the variable "msa" uniquely identifies an MSA in  the query:
# 'metro/tiger/2014/1/tl_2014_us' and data['t_name'] like 'msa%';

import timeit
from multiprocessing import Pool
import sqlalchemy as sa
import pandas as pd
from shapely.geometry import shape
from shapely.wkt import dumps
import geopandas as gpd
from pandasql import sqldf
from numpy import argmax


engine = sa.create_engine("crate://db.world.io:4200")

q = """
select data['t_namelsad'] namelsad, data['t_name'] name
  from world
 where ingestid = 'd02e240f56cb29b66e11201a1f5a39e0'
   and metadata['status'] is null
 order by data['i_seq'] LIMIT 50;
"""
msas = pd.read_sql(q, engine)
msas.head()

for id, rw in msas.iterrows():
    from rtree import index

    start_time = timeit.default_timer()

    msa = rw.namelsad
    msatag = rw['name']
    print('processing {}'.format(msatag))

    #####Get the MSA shape
    q = """select shape from world
    where datasetid='metro/tiger/2014/1/tl_2014_us'
    and data['t_namelsad'] like '{}%';
    """.format(msa)
    z = pd.read_sql(q, engine)
    BB = dumps(shape(z['shape'][0]))

    #### Get the Tiger blocks that intersect an MSA
    q = """select count(*) from world
        where ingestid = '25cb2427c0a5710a983e2f3a1f484bc6'
         and match(shape, '{}') using intersects""".format(BB)
    z = pd.read_sql(q, engine)

    q = """select shape, data['t_geoid10'] geoid10  from world
        where ingestid = '25cb2427c0a5710a983e2f3a1f484bc6'
         and match(shape, '{}') using intersects limit {}""".format(BB, z['count(*)'][0])
    blocks = pd.read_sql(q, engine)

    ##  Make a geodataframe wth the blocks
    geo = [shape(i) for i in blocks['shape']]
    del blocks['shape']
    blocks.gpd = gpd.GeoDataFrame(geometry=geo, data=blocks, crs={'init': 'epsg:4326'})

    ###### Get housing data
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
    #Make a geodataframe with the housing data
    geo = [shape(i) for i in houses['shape']]
    del houses['shape']
    houses.gpd = gpd.GeoDataFrame(geometry=geo, data=houses, crs={'init': 'epsg:4326'})

    #Select residential points only
    h = ['Single Family Residence','Condominium', 'Triplex (3 units, any combination)',
                                   'Multi-Family Dwellings (Generic, any combination)',
                                   'Duplex (2 units, any combination)', 'Quadplex (4 Units, Any Combination)',
                                   'Apartment house (5+ units)', 'Quadruplex (4 units, any combination)']

    houses.gpd = houses.gpd[houses.gpd['propertytype'].isin(h)]
    houses.gpd['type'] = 'residential'
    del houses.gpd['propertygroup']
    del houses.gpd['propertytype']

    ###### Get business data

    #Get ingestid and datasetid of the business data for the msa processed
    def bizLoc():
        #engine = sa.create_engine("crate://db.world.io:4200")
        # This query might fail.  make sure that the variable msa defined above can also uniquely identify the datasetid
        # of the business data in the desired MSA
        q = """select datasetid, metadata['table'] _table, ingestid, major, minor
        from dataset where datasetid like 'business/spatially/201705/2/metro-{}'
        order by datasetid, major desc, minor desc""".format(msatag)
        df = pd.read_sql(q, engine)
        df.head()
        x = df.loc[df.major == df.major.max()]
        x = x.loc[x.minor == x.minor.max()]
        x = x.reset_index(drop=True)
        out = {'ingestID':x.ingestid[0], 'table':x._table[0]}
        return(out)

    bl = bizLoc()
    q = """select count(*) from {}
            where ingestid = '{}'""".format(bl['table'], bl['ingestID'])
    z = pd.read_sql(q, engine)

    q = """
    select a.data['i_pid'] pid, a.shape, a.data['i_is_restaurant'] is_restaurant, a.data['t_sic'] sic
    from {} a
    where a.ingestid = '{}'
    limit {}
    """.format(bl['table'], bl['ingestID'], z['count(*)'][0]*10)


    biz = pd.read_sql(q, engine)
    geo = [shape(i) for i in biz['shape']]
    del biz['shape']
    biz.gpd = gpd.GeoDataFrame(geometry=geo, data=biz, crs={'init': 'epsg:4326'}).copy()

    ####label the business data into the types used in tapestry (uses SIC and is_restaurant)
    del biz['geometry']
    biz.gpd['type'] = ''

    pysqldf = lambda q: sqldf(q, globals())

    ###Professionals
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

    ###Retail
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

    ###Hotel
    q = """
    select pid from biz
    where sic like '701%';
    """
    k = pysqldf(q)
    key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
    biz.gpd.set_value(index=key, col='type', value='hotel')

    ###Culture and entertainment
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

    ###Government
    q = """
    select pid from biz
    where sic like '91%'
    or sic like '921%';
    """
    k = pysqldf(q)
    key = biz.gpd.loc[biz.gpd.pid.isin(tuple(k.pid))].index.tolist()
    biz.gpd.set_value(index=key, col='type', value='government')

    ###Restaurant
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

    ###### Intersection points w. blocks
    idx = index.Index()
    for index, row in blocks.gpd.iterrows():
        idx.insert(index,row.geometry.bounds)

    def seba_over(point):
        poly_idx = [i for i in idx.intersection((point.coords[0]))
                    if point.within(blocks.gpd.geometry[i])]
        if poly_idx:
            out = {'polyID':poly_idx[0]}
        else:
            out = {'polyID':-999}
        return(out)


    if __name__ == '__main__':
        with Pool(6) as p:
            poly_ids = p.map(seba_over, points.geometry)

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

    labels = pd.DataFrame(data={'type':pt.apply(argmax,1)})
    labels['geoid10'] = labels.index
    blocks.gpd = blocks.gpd.merge(labels, on='geoid10', how='left')
    blocks.gpd.to_csv('Tapestry_{}.txt'.format(msatag), sep='\t', index=False)
    elapsed = (timeit.default_timer() - start_time) / 60
    print('{} completed in {} minutes'.format(msatag, elapsed))
    print('----------------------------------------------------')
