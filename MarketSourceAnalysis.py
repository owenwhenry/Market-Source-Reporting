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
import datetime as dt
import sys
import matplotlib.pyplot as plt

#Initialize variables for time periods, as these will be used globally

today = dt.date.today()
yesterday = dt.date.today() + dt.timedelta(-1)

day = today.day
month = today.month
year = today.year

this_week_start = 0
this_week_end = 0

last_week_start = 0
last_week_end = 0

this_month_start = 0
this_month_end = 0

last_month_start = 0
last_month_end = 0

this_quarter_start = 0
this_quarter_end = 0

last_quarter_start = 0
last_quarter_end = 0

this_year_start = 0
this_year_end = 0

last_year_start = 0
last_year_end = 0

current_quarter = 0


#This tracks various values per table/piece of market source data
#This is called frequently in loops
ms_values_dict = {'audiences': ['audlong','audShort','audiences'],
                  'campaigns' : ['camplong', 'campShort', 'campaigns'],
                  'creatives' : ['creativelong', 'creativeShort', 'creatives'],
                  'fiscal_year' : ['fylong', 'fyShort', 'fiscal_years'],
                  'media' : ['mediumLong', 'mediumShort','media'],
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

#Sets the week
def set_week():
    try:
        global this_week_start
        global this_week_end
        global last_week_start
        global last_week_end
        
        this_week_start = today - dt.timedelta(dt.date.weekday(today))
        this_week_end = this_week_start + dt.timedelta(6)
        
        last_week_start = this_week_start - dt.timedelta(7)
        last_week_end = this_week_end - dt.timedelta(7)
        
    except Exception as e:
        print(e)        
        sys.exit()

#Sets the month
def set_month():
    next_month = today.replace(day=28) + dt.timedelta(days=4)
    
    global this_month_start
    global this_month_end
    global last_month_start
    global last_month_end
    
    this_month_start = dt.date(year, month, 1)
    this_month_end = next_month - dt.timedelta(next_month.day)
    
    last_month_end = this_month_start - dt.timedelta(1)
    last_month_start =  last_month_end - dt.timedelta(last_month_end.day -1)
    
#Sets the quarter, which at CRS runs October to September
def set_quarter():
    try:
        global current_quarter
        global this_quarter_start
        global this_quarter_end
        global last_quarter_start
        global last_quarter_end
        
        cur_day = dt.date.today()
        cur_month = int(cur_day.month -1)
        cur_quarter = int(cur_month//3)
        
        if cur_quarter == 0:
            current_quarter = "Q2"
            this_quarter_start = dt.date(year, 1, 1)
            this_quarter_end = dt.date(year, 3, 31)
            last_quarter_start = dt.date(year - 1, 10, 1)
            last_quarter_end = dt.date(year-1, 12, 31)
        elif cur_quarter == 1: 
            current_quarter = "Q3"   
            this_quarter_start = dt.date(year, 4, 1)
            this_quarter_end = dt.date(year, 6, 30)
            last_quarter_start = dt.date(year - 1, 1, 1)
            last_quarter_end = dt.date(year-1, 3, 31)            
        elif cur_quarter == 2:
            current_quarter = "Q4"
            this_quarter_start = dt.date(year, 7, 1)
            this_quarter_end = dt.date(year, 9, 30)
            last_quarter_start = dt.date(year - 1, 4, 1)
            last_quarter_end = dt.date(year - 1, 6, 31)            
        elif cur_quarter == 3:
            current_quarter = "Q1"
            this_quarter_start = dt.date(year, 10, 1)
            this_quarter_end = dt.date(year, 12, 31)
            last_quarter_start = dt.date(year - 1, 7, 1)
            last_quarter_end = dt.date(year-1, 9, 30)
        else:
            raise ValueError('Set Quarter Fail')
    
        
    except Exception as e:
        print(e)
        sys.exit()

#Sets the year
def set_year():
    global this_year_start
    global this_year_end

    global last_year_start
    global last_year_end
    
    this_year_start = dt.date(year, 1, 1)
    this_year_end = dt.date(year, 12, 31)
    
    last_year_start = dt.date(year - 1, 1, 1)
    last_year_end = dt.date(year - 1, 12, 31)

#Test method for making sure that the dates are right
def time_period_test():
    
    print("This week starts: %s"%this_week_start) 
    print("This week ends: %s"%this_week_end) 

    print("Last week started: %s"%last_week_start ) 
    print("Last week ended: %s" %last_week_end )

    print("This month starts: %s" %this_month_start )
    print("This month ends: %s" %this_month_end )
    
    print("Last month started: %s" %last_month_start )
    print("Last month ended: %s" %last_month_end )
    
    print("This quarter started: %s" %this_quarter_start )
    print("This quarter ended: %s"  %this_quarter_end )
    
    print("Last quarter started: %s" %last_quarter_start )
    print("Last quarter ended: %s" %last_quarter_end )
    
    print("This year starts: %s" %this_year_start )
    print("This year ends: %s" %this_year_end )
    
    print("Last year started: %s" %last_year_start )
    print("Last year ended: %s" %last_year_end )
    
    print("The current quarter is %s" %current_quarter)
    
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
    #print("Assembling dataframe based on query: ")
    #print("")
    #print(sql_query)
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
                raise ValueError('No merge column provided.')
                sys.exit()
            elif dataframe is None:
                raise ValueError('Dataframe parameter cannot be empty!')
            else:
                raise ValueError('Problem assembling dataframe')
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
        sys.exit()
        

def get_ms_data():
    try:
        #Connect to the EA Data Warehouse in Azure to pull transactions with codes
        ea_dw_cnxn = db_connect(cv.az_driver, 
                                cv.az_server,
                                cv.az_port,
                                cv.az_database,
                                cv.az_username,
                                cv.az_password)
            
        #Connect to the code generator database to pull metadata on codes
        ms_db_cnxn = db_connect(cv.cg_driver, 
                                cv.cg_server,
                                cv.cg_port,
                                cv.cg_database,
                                cv.cg_username,
                                cv.cg_password)
        
        ms_data_query = form_and_revenue_query + " WHERE " + ms_where_clause
    
        ms_df = frame_assembler(ms_data_query, ea_dw_cnxn)

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
            ms_df = frame_assembler(ms_query, ms_db_cnxn, 'merge', ms_df, ms_values_dict[value][1])
        
        return ms_df
    
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
        sys.exit()
    else:
        return name
    
#This creates my graphs by week
def tp_figure_maker(df, comparison_type = None ):
    #try:
        this_period_start = 0 
        this_period_end = 0
        last_period_start = 0
        last_period_end = 0
            
        if comparison_type == 'week':
            this_period_start = this_week_start
            this_period_end = this_week_end
            last_period_start = last_week_start
            last_period_end = last_week_end
        elif comparison_type == 'month':
            this_period_start = this_month_start
            this_period_end = this_month_end
            last_period_start = this_month_start
            last_period_end = this_month_end
        elif comparison_type == 'quarter':
            this_period_start = this_quarter_start
            this_period_end = this_quarter_end
            last_period_start = this_quarter_start
            last_period_end = this_quarter_end
        elif comparison_type == 'year':
            this_period_start = this_year_start
            this_period_end = this_year_end
            last_period_start = this_year_start
            last_period_end = this_year_end
        else:
            raise ValueError("Comparison Type Invalid - Please specify time period")

        print("I've set the time periods!")
        
        df['DateCreated'] = pandas.to_datetime(df['DateCreated'])
        
        df['Day_Of_Year'] = df['DateCreated'].dt.dayofyear
        
        df = df.set_index(['DateCreated'])
        df.sort_index(inplace=True, ascending=True)
        
        print("I've set the index!")
        
        print('Creating time period slices...')
        
        this_period_data = df.loc[this_period_start:this_period_end]
        print("This period's data: ")
        print(this_period_data.head())
        
        last_period_data = df.loc[last_period_start:last_period_end]
        
        print("Last period's data: ")
        print(last_period_data.head())       
        
        this_period_count = (this_period_data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                           agg('count'))
        
        last_period_count = (last_period_data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                           agg('count'))
        
        this_period_count.sort_index(inplace=True, ascending = True)
        last_period_count.sort_index(inplace=True, ascending = True)
        
        this_period_count.reset_index
        
        this_period_count.to_csv('this_period.csv')
        last_period_count.to_csv('last_period.csv')
        
        fig, ax = plt.subplots()
        
        ax.plot(this_period_count)
        ax.plot(last_period_count)
        ax.set_xlabel("Day")
        ax.set_ylabel("Count")
        plot = fig.get_figure()
        
    #except Exception as e:
        #print(e)

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
    print(summary_frame)
    summary_list = top_five(dataframe, column)
    print(summary_list)
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
    print('Setting dates...')
    set_week()
    set_month()
    set_quarter()
    set_year()
    
    print('Getting data...')
    df = get_ms_data()    
    
    print('Making figures...')
    
    tp_figure_maker(df, 'week')

    
    #Time to make some graphs!
    #figure_maker(ea_df,'platLong', 'MS_Platform_Summary.png')
    #figure_maker(ea_df, 'camplong', 'MS_Campaign_Summary.png')
    #figure_maker(ea_df, 'creativelong', 'MS_Creative_Summary.png')
    #figure_maker(ea_df, 'mediumlong', 'MS_Medium_Summary.png')
    #figure_maker(ea_df, 'progLong', 'MS_Program_Summary.png')

        
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