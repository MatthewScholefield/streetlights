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
from os import chdir
from petl import Table

all_attributes = [
    'PoleID', 'Longitude', 'Latitude', 'LightbulbType',
    'Wattage', 'Lumens', 'AttachedTech', 'LightAttributes',
    'FiberWiFiEnable', 'PoleType', 'PoleOwner', 'DataSource'
]


def geom_to_tuple(geom: str) -> tuple:
    """
    Args:
        geom: combined KCMO style point string ie. POINT (-94.574238674757 39.021251335024)
    Returns:
        latitude and longitude as floats
    """
    lat, long = geom.replace('POINT', '').strip('( )').split(' ')
    return float(lat), float(long)


def remove_empty(items: list) -> list:
    """Takes a list of items and returns elements that are not empty"""
    return [x for x in items if x is not None]


def prefix_value(prefix: str, value: str) -> str:
    """Prepends a prefix if a value isn't empty"""
    return (prefix + value) if value else ''


def find_wifi(*args) -> bool:
    """
    Args:
        args: Any number of fields
    Returns:
        Whether the fields indicate wifi
    """
    wifilist = ['Google', 'Sprint', 'Wireless', 'Mobil']
    for x in args:
        if x is not None:
            if any(word in x for word in wifilist):
                return True
    return False


def print_intro(table: Table, name: str):
    print('===', name, '===')
    print(table)
    print()


def kcmo_convert(filepath: str, xtrapath: str) -> Table:
    """
    Takes the both KCMO input datasets and
    converts to a universal format

    Args:
        filepath: Path to primary kcmo csv dataset
        xtrapath: Path to additional kcmo xlsx dataset
    Returns:
        Universal petl.Table object
    """
    kcmo = etl.fromcsv(filepath)
    kcx = etl.fromxlsx(xtrapath)
    table = etl.join(kcmo, kcx, lkey='POLEID', rkey='IDNumber')
    print_intro(table, 'KCMO')
    del kcmo
    del kcx

    for field, value in {
        'PoleID': lambda x: x['POLEID'],
        'Longitude': lambda x: geom_to_tuple(x['the_geom'])[0],
        'Latitude': lambda x: geom_to_tuple(x['the_geom'])[1],
        'LightbulbType': lambda x: x['LUMINAIRE TYPE'],
        'Wattage': lambda x: x['WATTS'],
        'Lumens': None,
        'LightAttributes': lambda x: remove_empty([
            x['ATTACHMENT 10'], x['ATTACHMENT 9'], x['ATTACHMENT 8'],
            x['ATTACHMENT 7'], x['ATTACHMENT 6'], x['ATTACHMENT 5'],
            x['ATTACHMENT 4'], x['ATTACHMENT 3'], x['ATTACHMENT 2'],
            x['ATTACHMENT 1'], x['SPECIAL_N2'], x['SPECIAL_NO']
        ]),
        'AttachedTech': lambda x: bool(x['LightAttributes']),
        'FiberWiFiEnable': lambda x: find_wifi(
            *x['LightAttributes'], x['SPECIAL_N2'], x['SPECIAL_NO']
        ),
        'PoleType': lambda x: x['POLE TYPE'],
        'PoleOwner': lambda x: x['POLE OWNER'],
        'DataSource': 'Kansas City'
    }.items():
        table = etl.addfield(table, field, value)

    return etl.cut(table, all_attributes)


def lee_convert(filepath: str) -> Table:
    """
    Converts lee summit's dataset into a universal csv

    Args:
        filepath: Path to lee summit csv
    Returns:
        Universal petl.Table object
    """
    table = etl.fromcsv(filepath)
    print_intro(table, 'Lee Summit')
    for field, value in {
        'PoleID': lambda x: 'KCLEE-' + x['OBJECTID'],
        'Longitude': lambda x: x['POINT_X'],
        'Latitude': lambda x: x['POINT_Y'],
        'LightbulbType': lambda x: x['LAMPTYPE'],
        'Wattage': lambda x: x['WATTS'],
        'Lumens': lambda x: x['LUMENS'],
        'AttachedTech': False,
        'LightAttributes': lambda x: x['FIXTURETYP'],
        'FiberWiFiEnable': False,
        'PoleType': None,
        'PoleOwner': 'Lee Summit',
        'DataSource': 'Lee Summit'
    }.items():
        table = etl.addfield(table, field, value)
    return etl.cut(table, all_attributes)


def kcpl_convert(filepath: str) -> Table:
    """
    Converts KCPL's dataset into a universal csv

    Args:
        filepath: Path to KCPL's csv
    Returns:
        Universal petl.Table object
    """
    table = etl.fromcsv(filepath)
    print_intro(table, 'KCPL')
    for field, value in {
        'PoleID': lambda x: x['POLEID'],
        'Longitude': lambda x: x['X-COORD'],
        'Latitude': lambda x: x['Y-COORD'],
        'LightbulbType': None,
        'Wattage': None,
        'Lumens': None,
        'AttachedTech': False,
        'LightAttributes': lambda x: remove_empty([
            prefix_value('GeoSource-', x['GEO_SOURCE']),
            prefix_value('RetiredDate-', x['RETIRED_DATE'])
        ]),
        'FiberWiFiEnable': False,
        'PoleType': None,
        'PoleOwner': 'KCPL',
        'DataSource': 'KCPL'
    }.items():
        table = etl.addfield(table, field, value)
    return etl.cut(table, all_attributes)


def main():
    # for testing
    chdir('data')
    etl.tocsv(
        kcpl_convert('kcpl-mo-ks.csv'),
        'kcpl-mo-ks.clean.csv'
    )
    etl.tocsv(
        kcmo_convert('kansas-city-mo.csv', 'kansas-city-mo-extra.xlsx'),
        'kansas-city-mo.clean.csv'
    )
    etl.tocsv(
        lee_convert('lee-summit-mo.csv'),
        'lee-summit-mo.clean.csv'
    )
    print('done')


if __name__ == '__main__':
    main()
