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
from datetime import date
from datetime import time


#Initialize variables for the quarters, as these will be used globally
current_quarter = 0
quarter_start_date = 0
quarter_end_date = 0
prev_year_quarter_start = 0
prev_year_quarter_end = 0
this_year = 0
last_year = 0

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

quarter_date_clause = '''
DateCreated >= %s and DateCreated <= %s
''' %(quarter_start_date, quarter_end_date)

#Sets the current quarter for the purposes of defining bounds of data frames
    
def set_quarter():
    try:
        global current_quarter
        global quarter_start_date
        global quarter_end_date
        global prev_year_quarter_start
        global prev_year_quarter_end
        global this_year
        global last_year
        cur_day = date.today()
        cur_month = int(cur_day.month -1)
        cur_quarter = int(cur_month//3)
        this_year = int(cur_day.year)
        last_year = this_year - 1
        if cur_quarter == 0:
            current_quarter = "Q2"
            quarter_start_date = date(this_year, 1, 1)
            quarter_end_date = date(this_year, 3, 31)
            prev_year_quarter_start = date(last_year, 1, 1)
            prev_year_quarter_end = date(last_year, 3, 31)
        elif cur_quarter == 1: 
            current_quarter = "Q3"   
            quarter_start_date = date(this_year, 4, 1)
            quarter_end_date = date(this_year, 6, 30)
            prev_year_quarter_start = date(last_year, 4, 1)
            prev_year_quarter_end = date(last_year, 6, 30)            
        elif cur_quarter == 2:
            current_quarter = "Q4"
            quarter_start_date = date(this_year, 7, 1)
            quarter_end_date = date(this_year, 9, 30)
            prev_year_quarter_start = date(last_year, 7, 1)
            prev_year_quarter_end = date(last_year, 9, 30)
        elif cur_quarter == 3:
            current_quarter = "Q1"
            quarter_start_date = date(this_year, 10, 1)
            quarter_end_date = date(this_year, 12, 31)
            prev_year_quarter_start = date(last_year, 10, 1)
            prev_year_quarter_end = date(last_year, 12, 31)
        else:
            print('Error - Set Quarter Fail')
            quit()
    except Exception as e:
        print(e)
#    else:
#        quarter_start_date = prev_year_quarter_end.strftime('%Y-%m-%d')
#        quarter_end_date = prev_year_quarter_end.strftime('%Y-%m-%d')
#        prev_year_quarter_start = prev_year_quarter_end.strftime('%Y-%m-%d')
#        prev_year_quarter_end = prev_year_quarter_end.strftime('%Y-%m-%d')
        
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
def frame_assembler(sql_query, cnxn, update_type = None, 
                    dataframe = None, mergecol = None):
    try:
        new_dataframe = pandas.read_sql(sql_query, cnxn)
        if update_type == 'merge':
            if dataframe is not None and mergecol is not None:
                updated_frame = pandas.merge(dataframe,
                                             new_dataframe,
                                             on = mergecol,
                                             how = 'left'
                                             )
                return updated_frame
            elif dataframe is not None and mergecol is None:
                print('Error - no merge column provided.')
                quit()
            elif dataframe is None:
                print('Error - dataframe parameter cannot be empty!')
            else:
                print('Error - problem assembling frame')
        elif update_type == 'append':
            if dataframe is not None:
                updated_frame = dataframe.append(new_dataframe, 
                                                 ignore_index = True)
                return updated_frame
            elif dataframe is None:
                print('Error - dataframe parameter cannot be empty!')
            else:
                print('Error - problem assembling frame')
        else:
            return new_dataframe
    except Exception as e:
        print(e)
#This handles some small discrepancies in the data - some of the column 
#names are upper case, some are lower case, and some are numbers.
#This difference in naming conventions created a small challenge when
#matching between Azure and the Code Generator, so this method enforces
#lower case to maintain a good match between both data sources.
#It then takes the existing EA Dataframe of transactions and puts it
#together with the existing metadata to produce a dataframe with all values
#necessary for reporting
def add_ms_values(dataframe, ms_db_connection):
      for value in ms_values_dict.keys():
        if value == 'fiscal_year' :
            ms_query = 'SELECT ' + ms_values_dict[value][0] + ', ' + ms_values_dict[value][1]
            ms_query += ' from ' + ms_values_dict[value][2]
        else: 
            ms_query = 'SELECT ' + ms_values_dict[value][0] + ', ' + 'LCASE(' + ms_values_dict[value][1] + ')'
            ms_query += ' as ' + ms_values_dict[value][1] + ' from ' + ms_values_dict[value][2]
        dataframe = frame_assembler(ms_query, ms_db_connection, 'merge', dataframe, ms_values_dict[value][1])

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

#This creates my graphs by week
def week_figure_maker(base_query, db_connection):
    this_year_clause = ("COF.DateCreated >= '%s' and COF.DateCreated <= '%s'" 
                                %(quarter_start_date, quarter_end_date))
    last_year_clause = ("COF.DateCreated >= '%s' and COF.DateCreated <= '%s'"
                                %(prev_year_quarter_start, prev_year_quarter_end)) 
                           
    this_year_query = (base_query + ' WHERE ' + this_year_clause)
    last_year_query = (base_query + ' WHERE ' + last_year_clause)
    this_year_data = frame_assembler(this_year_query, db_connection)
    last_year_data = frame_assembler(last_year_query, db_connection)
    this_year_data['DateCreated'] = pandas.to_datetime(this_year_data['DateCreated'])
    last_year_data['DateCreated'] = pandas.to_datetime(last_year_data['DateCreated'])
    this_year_data['Day_Of_Year'] = this_year_data['DateCreated'].dt.dayofyear
    last_year_data['Day_Of_Year'] = last_year_data['DateCreated'].dt.dayofyear
    this_year_count = (this_year_data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                       agg('count').sort_values(ascending = False))
    last_year_count = (last_year_Data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                       agg('count').sort_values(ascending = False))
    this_year_count.to_csv('This_year.csv')
    last_year_count.to_csv('Last_year.csv')
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
    #set the quarter
    set_quarter()

    #Connect to the EA Data Warehouse in Azure to pull transactions with codes
    ea_dw_connection = db_connect(cv.az_driver, 
                                  cv.az_server,
                                  cv.az_port,
                                  cv.az_database,
                                  cv.az_username,
                                  cv.az_password)
    
    #Connect to the code generator database to pull metadata on codes
    ms_db_connection = db_connect(cv.cg_driver, 
                                  cv.cg_server,
                                  cv.cg_port,
                                  cv.cg_database,
                                  cv.cg_username,
                                  cv.cg_password)
    

    
    
    #Run this bit if you want a CSV generated to check values, otherwise leave
    #it commented out.
    #ea_df.to_csv('MS_Output.csv')
    
    #Time to make some graphs!
    #figure_maker(ea_df,'platLong', 'MS_Platform_Summary.png')
    #figure_maker(ea_df, 'camplong', 'MS_Campaign_Summary.png')
    #figure_maker(ea_df, 'creativelong', 'MS_Creative_Summary.png')
    #figure_maker(ea_df, 'mediumlong', 'MS_Medium_Summary.png')
    #figure_maker(ea_df, 'progLong', 'MS_Program_Summary.png')
    
    week_figure_maker(form_and_revenue_query, ea_dw_connection)
    

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