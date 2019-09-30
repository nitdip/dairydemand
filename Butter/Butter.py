# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 08:46:12 2019

@author: jvalderama
"""
import os

os.chdir('..')

path = os.curdir

from USDA import GATS, QuickStats
#
product = 'Butter'

import_file = f'imports.csv'
import_codes = [405002000,
               405004000,
               405101000,
               405102000]

gats_imports = GATS(import_file, kind='imports', codes=import_codes, product=product)
imports = gats_imports.transform()
gats_imports.save(f'{path}/{product}/{product}Imports')

export_file = f'exports.csv'
export_codes = [405005000,
               405100000,
               405105000]

gats_exports = GATS(export_file, kind='exports', codes=export_codes, product=product)
exports = gats_exports.transform()
gats_exports.save(f'{path}/{product}/{product}Exports')

items = ['BUTTER']
stock_file = f'{path}/{product}/stock.csv'
qs_stocks = QuickStats(stock_file, kind='stocks', items=items)
qs_stocks.save(f'{path}/{product}/{product}Stocks')

data_items = ['BUTTER']
prod_file = f'{path}/{product}/production.csv'
qs_production = QuickStats(prod_file, kind='production', items=data_items)
qs_production.save(f'{path}/{product}/{product}Production')
