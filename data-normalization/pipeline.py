#!/usr/bin/env python3
"""
This is an etl pipeline and 
is not intended to be used as part of the 
webapplication. The purpose of a pipeline
is to have uniform data format we can use
for bulk loading the db. If we get random flat
files we can feed them through a pipeline.

author: Chris Whitehead
github: cgwhitehead

TODO:
    *Identify any function that could be reused for multiple piplines
    **move to a central pipline function area
    *Work with SMEs to see if we are understanding light posts correctly. 
"""

import petl as etl
#FOR TESTING
#geom='POINT (-94.574238674757 39.021251335024)'
def geom_to_tuple(geom):
    """
    Takes a lat/long point (or geom) from KCMO style csvs.
    Returns (lat, long) tuple
    """
    geom=geom[6:]
    geom=geom.replace(" ", ", ")
    return eval(geom)

def make_a_list(*args):
    """
    Takes any number of fields
    return list of attachements or other feature
    """
    attached=[]
    for x in args:
        if x != None:
            attached.append(x)
    return attached
    
def find_wifi(*args):
    """
    Takes any number of fields
    Looks for wifi indicators
    returns Bool
    """
    wifilist=['Google', 'Sprint', 'Wireless', 'Mobil']
    for x in args:
        if x!=None:
            if any(word in x for word in wifilist):
                return True
    return False
        

def kcmo_convert(filepath, xtrapath):
    """
    Takes the file path to a csv in the format used by Kansas City proper
    converts to universal format 
    outputs csv.
    """
    kcmo=etl.fromcsv(filepath)
    kcx=etl.fromxlsx(xtrapath)
    kcjoin=etl.join(kcmo, kcx, lkey='POLEID', rkey='IDNumber')
    del kcmo
    del kcx
    
    kcjoin=etl.addfield(kcjoin, 'PoleID', lambda x: x['POLEID'])
    kcjoin=etl.addfield(kcjoin, 'Longitude', lambda x: geom_to_tuple(x['the_geom'])[0])
    kcjoin=etl.addfield(kcjoin, 'Latitude', lambda x: geom_to_tuple(x['the_geom'])[1])
    kcjoin=etl.addfield(kcjoin, 'LightbulbType', lambda x: x['LUMINAIRE TYPE'])
    kcjoin=etl.addfield(kcjoin, 'Wattage', lambda x: x['WATTS'])
    kcjoin=etl.addfield(kcjoin, 'Lumens', None)
    kcjoin=etl.addfield(kcjoin, 'LightAttributes',lambda x: make_a_list(x['ATTACHMENT 10']
                                                 ,x['ATTACHMENT 9']
                                                 ,x['ATTACHMENT 8']
                                                 ,x['ATTACHMENT 7']
                                                 ,x['ATTACHMENT 6']
                                                 ,x['ATTACHMENT 5']
                                                 ,x['ATTACHMENT 4']
                                                 ,x['ATTACHMENT 3']
                                                 ,x['ATTACHMENT 2']
                                                 ,x['ATTACHMENT 1']
                                                 ,x['SPECIAL_N2']
                                                 ,x['SPECIAL_NO']
                                                 ))
    kcjoin=etl.addfield(kcjoin, 'AttachedTech', lambda x: bool(x['LightAttributes']))
    kcjoin=etl.addfield(kcjoin, 'FiberWiFiEnable', lambda x: find_wifi(x['LightAttributes']
                                                                        ,x['SPECIAL_N2']
                                                                        ,x['SPECIAL_NO']
                                                                        ))
    kcjoin=etl.addfield(kcjoin, 'PoleType', lambda x: x['POLE TYPE'])
    kcjoin=etl.addfield(kcjoin, 'PoleOwner', lambda x: x['POLE OWNER'])
    kcjoin=etl.addfield(kcjoin, 'DataSource', 'Kansas City')
    kcjoin=etl.cut(kcjoin,'PoleID', 'Longitude', 'Latitude', 'LightbulbType', 
                   'Wattage', 'Lumens', 'AttachedTech', 'LightAttributes',
                   'FiberWiFiEnable', 'PoleType', 'PoleOwner', 'DataSource')
    etl.tocsv(kcjoin, 'data/kcmo_clean.csv')
    
def main():
        #for testing 
    filepath='data/kansas-city-mo.csv'
    xtrapath='data/kansas-city-mo-extra.xlsx'
    kcmo_convert(filepath, xtrapath)
    print('done')
    

if __name__ == '__main__':
    main()
    
    