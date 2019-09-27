import pandas as pd
import numpy as np

class GATS(object):
    '''
    Turns GATS data into tidy format
    '''
    
    def __init__(self, filename, kind, codes, product):
        '''
        Parameters
        ----------
        filename (str): csv file to clean
        kind (str): import or export
        codes (int): list of codes to filter the dataframe against
        product (str): product name. IE Butter, Cheddar, NFDM
        '''
        self.filename = filename
        self.kind = kind.title()
        self.codes = codes
        self.product = product.title()
        self.df = pd.read_csv(filename, skiprows=4)


    def melt(self):
        '''
        Combines month columns into a single column called Month.
        
        Returns
        -------
        dataframe
        '''
        df = self.df
        months = [str(x) for x in range(1, 13)]
        old_columns = list(df.columns[df.columns.str.contains('Qty')][:12])
        datezip = dict(zip(old_columns, months))
        df.rename(columns=datezip, inplace=True)

        melted = df.melt(id_vars=['Product Code', 'Product', 'Year'], 
                              value_vars=months, var_name='Month', 
                              value_name=self.kind)
        melted['Date'] = pd.to_datetime(melted['Year'].str[-4:] +
                   melted['Month'], format='%Y%m')
        
        self.df = melted
        
        return self.df


    def filterCodes(self):
        '''
        Filter dataframe to contain only needed products by code
        
        Returns
        -------
        dataframe
        '''
        
        df = self.df
        codes = self.codes
        df_new = df[df['Product Code'].isin(codes)]
        df_new['Product'] = self.product
        self.df = df_new
        
        return self.df
    
    def pivot(self):
        '''
        Pivots Product into tidy format so each row is an observation
        
        Returns
        -------
        dataframe
        '''
        
        df = self.df
        pivoted = df.pivot_table(index='Date', columns='Product', 
                                 values=self.kind, aggfunc=np.sum)
        self.df = pivoted
        
        return self.df
    
    def string_to_int(self):
        '''
        Converts string to integer then converts MT to lbs.
        '''
        
        df = self.df
        kind = self.kind
        df[kind] = df[kind].str.replace(',','')
        df[kind] = df[kind].astype(float).mul(2204.623).round()
        self.df = df
    
    def transform(self):
        '''
        Transforms data into tidy format for analysis
        
        Returns
        -------
        dataframe
        '''

        gats = GATS(self.filename, self.kind, self.codes, self.product)
        gats.filterCodes()
        gats.melt()
        gats.string_to_int()
        gats.pivot()
        
        return gats.df
    
    def save(self, title):
        '''
        Saves dataframe as a CSV
        
        Parameters
        ----------
        title (str): name of the file
        '''
        
        df = self.transform()
        df.to_csv(f'{title}.csv')
        
    
class QuickStats(object):
    '''
    Clean USDA QuickStats data for analysis.
    '''
    
    def __init__(self, filename, kind, items=None):
        self.filename = filename
        self.df = pd.read_csv(self.filename).dropna(how='all', axis=1)
        self.kind = kind.title()
        self.items = items
        
    def value_conversion(self, inplace=False):
        '''
        Converts string to integer
        
        Returns
        -------
        dataframe
        '''
        df = self.df
        df['Value'] = df['Value'].str.replace(',','')
        df['Value'] = df['Value'].astype(float).round()
        if self.kind == 'Stocks':
            df['Beg Stock'] = df['Value'].shift(1)
        df.columns = df.columns.str.replace('Value', self.kind)
        
        if inplace is True:
            self.df = df        
        
        return df   
        
    
    def date(self, inplace=False):
        '''
        Creates a Date column
        
        Returns
        -------
        dataframe
        '''
        df = self.df
        months_int = [x+1 for x in range(12)]
        months_str = df['Period'].unique().tolist()
        if 'YEAR' in months_str:
            months_str.remove('YEAR')
        datezip = dict(zip(months_str, months_int))
        df['Period'].replace(datezip, inplace=True)
        df = df[df['Period'] != 'YEAR']
        df['Date'] = pd.to_datetime(df['Year'].map(str) + df['Period'].map(str), 
          format='%Y%m')
        df.sort_values(by='Date', inplace=True)
        
        if inplace is True:
            self.df = df
        
        return df
    
    def stockpivot(self, inplace=False):
        '''
        Converts rows into columns then renames the rows based on the unique
        values of the rows.
        
        Parameters
        ----------
        inplace (bool): Change the dataframe attribute to relfect the change
        
        Returns
        -------
        dataframe
        '''
        
        
        qs = QuickStats(self.filename, kind=self.kind, items=self.items)
        df = qs.date()        
        items_upper = [item.upper() for item in self.items]
        data_items = df['Data Item'].unique().tolist()
        datazip = dict(zip(data_items, items_upper))
        df['Data Item'].replace(datazip, inplace=True)
        pivot = df.pivot(index='Date', columns='Data Item', values='Value').reset_index()
        for item in items_upper:
            pivot[f'{item} Beg Stock'] = pivot[item].shift(1)
        
        if inplace is True:
            self.df = pivot
        
        return pivot
    
    def productpivot(self, inplace=False):
        '''
        Renames items in Data Items column then pivots the product column to 
        tidy format.
        
        Returns
        -------
        dataframe
        '''
        df = self.df
        data_items = df['Data Item'].unique().tolist()
        data_items_dict = dict(zip(data_items, self.items))
        df['Data Item'].replace(data_items_dict, inplace=True)
        pivoted = df.pivot(index='Date', columns='Data Item', values=self.kind).fillna(0)
        
        if inplace is True:
            self.df = pivoted
            return self.df
            
        return pivoted
    # removed pivot argument
    def transform(self):
        '''
        Transforms data from QuickStats into tidy format for loading into 
        Tableau.
        
        Parameters
        ----------
        pivot (bool): pivot the data. Default is False.
        
        Returns
        -------
        dataframe
        '''
        qs = QuickStats(self.filename, self.kind, self.items)
        qs.date(inplace=True)
        qs.value_conversion(inplace=True)
        
        if self.kind == 'Stocks' or self.kind == 'Stock':        
            return qs.stockpivot(inplace=True)
        elif self.kind == 'Production':
            return qs.productpivot()
#        else:
#            return qs.df[['Date', self.kind]]
            
    
    def save(self, title):
        df = self.transform()
        #df.set_index('Date', inplace=True)
        df.to_csv(f'{title}.csv')
        
        return df