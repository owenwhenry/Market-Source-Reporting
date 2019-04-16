# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 16:42:21 2019

@author: owen.henry
"""

#Pyodbc is used to connect to various databases
from pyodbc import connect
#CespanarVariables is my own script to track variables like database names and 
#drivers between scripts. For general use this needs to be converted to something 
#far more secure, but works for the moment
import Cespanar_variables as cv
#Pandas combines the data and runs analysis
import pandas

#This tracks various values per table/piece of market source data
#This is called frequently in loops
ms_values_dict = {'audiences': ['audlong','audShort','audiences'],
                  'campaigns' : ['camplong', 'campShort', 'campaigns'],
                  'creatives' : ['creativelong', 'creativeShort', 'creatives'],
                  'fiscal_year' : ['fylong', 'fyShort', 'fiscal_years'],
                  'media' : ['mediumlong', 'mediumShort','media'],
                  'platforms' : ['platLong','platShort','platforms'],
                  'programs' : ['progLong', 'progShort','programs']
                  }

form_and_revenue_query = '''
SELECT COF.ContactsOnlineFormID,
	COF.VanID,
	COF.OnlineFormID,
	CONVERT(date, COF.DateCreated) as DateCreated,
	IsNewContact,
	BatchEmailJobDistributionID,
	MarketSource,
	Amount,
	LOWER(SUBSTRING(MarketSource, 1, 3)) as 'progShort',
	LOWER(SUBSTRING(MarketSource, 4, 3)) as 'platShort',
	LOWER(SUBSTRING(MarketSource, 7, 2)) as 'mediumShort',
	LOWER(SUBSTRING(MarketSource, 9, 2)) as 'fyShort',
	LOWER(SUBSTRING(MarketSource, 11, 3)) as 'campShort',
	LOWER(SUBSTRING(MarketSource, 14, 2)) as 'audShort',
	LOWER(SUBSTRING(MarketSource, 16, 3)) as 'creativeShort',
	LOWER(SUBSTRING(MarketSource, 19, 2)) as 'Iteration'
FROM [dbo].[CRS_ContactsOnlineForms] COF
LEFT JOIN [dbo].[CRS_ContactsContributions] CC ON COF.ContactsOnlineFormID = CC.ContactsOnlineFormID
LEFT JOIN [dbo].[CRS_ContactsContributionsCodes] CCC ON CC.ContactsContributionID = CCC.COntactsContributionID
LEFT JOIN [dbo].[CRS_Codes] C on C.CodeID = CCC.CodeID
'''

ms_where_clause = '''
MarketSource IS NOT NULL
AND LEN(MarketSource) = 20
'''



#This method creates a database connection given the requisite variables
def db_connect(driver, server, port, database, username, password):
    connect_statement='DRIVER='+driver+';SERVER='+server+';PORT='+str(port) 
    connect_statement+=';DATABASE='+database+';UID='+username+';PWD='+password
    cnxn = connect(connect_statement)
    return cnxn

#This method creates a dataframe from a database connection and a query.
#If a second dataframe and mergecolumn is provided, it updates the frame based
#on the data in the query. This is useful for when you have matching data in
#multiple databases.
def frame_assembler(sql_query, cnxn, dataframe = None, mergecol = None):
    try:
        new_dataframe = pandas.read_sql(sql_query, cnxn)
        if dataframe is not None and mergecol is not None:
            updated_frame = pandas.merge(dataframe,
                                         new_dataframe,
                                         on = mergecol,
                                         how = 'left'
                                         )
            return updated_frame
        elif dataframe is not None and mergecol is not None:
            print('Error - no merge column provided.')
            quit()
        else:
            return new_dataframe
    except Exception as e:
        print(e)
        
#This method takes a dataframe and other information and outputs a graph as
#a file. This will eventually be converted to add images to a pdf. 
def figure_maker(dataframe, group_col, name, agg_method = 'count', plot_kind = 'bar'):
    try:
        plot_base = dataframe.groupby([group_col])[group_col].agg(agg_method).sort_values(ascending = False)
        plot = plot_base.plot(kind=plot_kind, figsize = (10,10))
        fig = plot.get_figure()
        fig.savefig(name)
    except Exception as e:
        print(e)
    else:
        return name

#This method returns the 5 most frequent items in a given column of a dataframe
#It's used when we want to limit what's displayed in the graph.
#By default, it returns a list of the top 5 items.
#If you pass 'dataframe' to the list_or_frame parameter, it gives you back
#a dataframe restricted to only data corresponding with those 5 observations.
def top_five(dataframe, column, list_or_frame = 'list'):
    top_five = []
    column_value_array = dataframe[column].value_counts()
    iter_item = 0
    for item in column_value_array.index:
        top_five.append(item)
        iter_item += 1
        if iter_item == 5:
            break
    if list_or_frame == 'list':
        return top_five
    elif list_or_frame == 'dataframe':
        new_frame = dataframe[dataframe[column].isin(top_five)]
        return new_frame
    else:
        raise ValueError('Invalid value for col_or_frame')

#This method is meant to spit out, for a given column, summary graphs for each
#of the top 5 items in that category. It's used to get a better picture of what
#data we're getting in well-defined groups, such as medium or platform.
#This method is still a work in progress.
def summary_graphs(dataframe, column):
    summary_frame = top_five(dataframe, column, 'dataframe')
    summary_list = top_five(dataframe, column)
    for key in ms_values_dict.keys():
        for value in summary_list:
            string_key = str(key)
            string_value = str(value)
            print('Executing summary_graphs for %s, %s') %(string_key, string_value)
            df = top_five(summary_frame, ms_values_dict[key][0], 'dataframe')
            figure_title = 'Top_5_%s_for_%s' %(key, value)
            figure_maker(df, ms_values_dict[key][0], figure_title)
            print(key)
            print(value)

#Main method, where the magic happens
def main():
    #Specify what table to query. I created a view in the database to make
    #this a bit simpler, but you could just as easily run the query here.
    ea_df_query = 'SELECT * FROM [dbo].CRS_Market_Source_2WK'
    
    #Connect to the EA Data Warehouse in Azure to pull transactions with codes
    ea_dw_connection = db_connect(cv.az_driver, 
                                  cv.az_server,
                                  cv.az_port,
                                  cv.az_database,
                                  cv.az_username,
                                  cv.az_password)
    
    #Create the dataframe.
    ea_df = frame_assembler(ea_df_query, ea_dw_connection)
    
    #Connect to the code generator database to pull metadata on codes
    ms_db_connection = db_connect(cv.cg_driver, 
                                  cv.cg_server,
                                  cv.cg_port,
                                  cv.cg_database,
                                  cv.cg_username,
                                  cv.cg_password)
    
    #This handles some small discrepancies in the data - some of the column 
    #names are upper case, some are lower case, and some are numbers.
    #This difference in naming conventions created a small challenge when
    #matching between Azure and the Code Generator, so this method enforces
    #lower case to maintain a good match between both data sources.
    #It then takes the existing EA Dataframe of transactions and puts it
    #together with the existing metadata to produce a dataframe with all values
    #necessary for reporting
    for value in ms_values_dict.keys():
        if value == 'fiscal_year' :
            ms_query = 'SELECT ' + ms_values_dict[value][0] + ', ' + ms_values_dict[value][1]
            ms_query += ' from ' + ms_values_dict[value][2]
        else: 
            ms_query = 'SELECT ' + ms_values_dict[value][0] + ', ' + 'LCASE(' + ms_values_dict[value][1] + ')'
            ms_query += ' as ' + ms_values_dict[value][1] + ' from ' + ms_values_dict[value][2]
        ea_df = frame_assembler(ms_query, ms_db_connection, ea_df, ms_values_dict[value][1] )
    
    #Run this bit if you want a CSV generated to check values, otherwise leave
    #it commented out.
    #ea_df.to_csv('MS_Output.csv')
    
    #Time to make some graphs!
    figure_maker(ea_df,'platLong', 'MS_Platform_Summary.png')
    figure_maker(ea_df, 'camplong', 'MS_Campaign_Summary.png')
    figure_maker(ea_df, 'creativelong', 'MS_Creative_Summary.png')
    figure_maker(ea_df, 'mediumlong', 'MS_Medium_Summary.png')
    figure_maker(ea_df, 'progLong', 'MS_Program_Summary.png')
    
#    top_5_medium_frame = ea_df[ea_df['mediumlong'].isin(top_five(ea_df, 'mediumlong'))]
        
#    figure_maker(top_5_medium_frame, 'mediumlong', 'MS_Medium_Top5_Summary.png')
    
#    summary_graphs(ea_df, 'mediumlong')
    
    #Summary totals to assemble:
        #Total Form Submissions - total for this week, total for last week, Week-over-week percent change, total this week last year, year-over-year percent change
        #New Contacts - total for this week, total for last week, week-over-week percent change, total this week last year, year over year percent change
        #Total raised - total for this week, total for last week, week-over-week percent change, total this week last year, year over year percent change
        #Emails sent - total for this week, total for last week, week-over-week percent change, total this week last year, year over year percent change
        #Email recipients - total for this week, total for last week, week-over-week percent change, total this week last year, year over year percent change
        #Market Source Transactions - total this week, total last week, week-over-week percent change, total this week last year, year over year percent change
    
    #Graphs to assemble:
        #Form Submissions by Week, current quarter - one line for weeks this year, one line for weeks last year, one line for last year average
        #Emails by week, current quarter - one line for weeks this year, one line for weeks last year, one line for last year average
        #New Contacts by week, current quarter - one line for weeks this year, one line for weeks last year, one line for last year average
        #Total raised by week, current quarter - one line for weeks this year, one line for weeks last year, one line for last year average
        
    
    
main()