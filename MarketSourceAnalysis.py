# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 16:42:21 2019

@author: owen.henry
"""

from __future__ import division
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
import jinja2


#Initialize variables for time periods, as these will be used globally
this_period_start = ''
this_period_end = ''
last_period_start = ''
last_period_end = ''

today = dt.date.today()
yesterday = dt.date.today() + dt.timedelta(-1)

day = today.day
month = today.month
year = today.year

current_quarter = ''


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

email_query = '''
SELECT A.EmailMessageID,
	EmailMessageDistributionID,
	DateSent,
	DateScheduled,
	EmailMessageName,
	EmailSubject
FROM CRS_EmailMessageDistributions A
INNER JOIN CRS_EmailMessages B ON A.EmailMessageID = B.EmailMessageID
WHERE A.EmailMessageID NOT IN (
		884,
		885
		)
'''

def set_time_period(period, verbose = False):
    try:
        global this_period_start
        global this_period_end
        global last_period_start
        global last_period_end
        global current_quarter
        
        if period == 'week':
            this_period_start = today - dt.timedelta(dt.date.weekday(today))
            this_period_end = this_period_start + dt.timedelta(6)
        
            last_period_start = this_period_start - dt.timedelta(7)
            last_period_end = this_period_end - dt.timedelta(7)
            
            if verbose == True:
                print("This week starts: %s"%this_period_start) 
                print("This week ends: %s"%this_period_end) 

                print("Last week started: %s"%last_period_start ) 
                print("Last week ended: %s" %last_period_end )
        if period == 'month':
            next_month = today.replace(day=28) + dt.timedelta(days=4)
            
            this_period_start = dt.date(year, month, 1)
            this_period_end = next_month - dt.timedelta(next_month.day)
            
            last_period_end = this_period_start - dt.timedelta(1)
            last_period_start =  last_period_end - dt.timedelta(last_period_end.day -1) 
            
            if verbose == True:
                print("This month starts: %s" %this_period_start )
                print("This month ends: %s" %this_period_end )
    
                print("Last month started: %s" %last_period_start )
                print("Last month ended: %s" %last_period_end )
        if period == 'quarter':
            cur_month = int(today.month -1)
            cur_quarter = int(cur_month//3)
            if cur_quarter == 0:
                current_quarter = "Q2"
                this_period_start = dt.date(year, 1, 1)
                this_period_end = dt.date(year, 3, 31)
                last_period_start = dt.date(year - 1, 10, 1)
                last_period_end = dt.date(year-1, 12, 31)
            elif cur_quarter == 1: 
                current_quarter = "Q3"   
                this_period_start = dt.date(year, 4, 1)
                this_period_end = dt.date(year, 6, 30)
                last_period_start = dt.date(year - 1, 1, 1)
                last_period_end = dt.date(year-1, 3, 31)            
            elif cur_quarter == 2:
                current_quarter = "Q4"
                this_period_start = dt.date(year, 7, 1)
                this_period_end = dt.date(year, 9, 30)
                last_period_start = dt.date(year - 1, 4, 1)
                last_period_end = dt.date(year - 1, 6, 31)            
            elif cur_quarter == 3:
                current_quarter = "Q1"
                this_period_start = dt.date(year, 10, 1)
                this_period_end = dt.date(year, 12, 31)
                last_period_start = dt.date(year - 1, 7, 1)
                last_period_end = dt.date(year-1, 9, 30)
            else:
                raise ValueError('Set Quarter Fail')
            
            if verbose == True:
                print("This quarter started: %s" %this_period_start )
                print("This quarter ended: %s"  %this_period_end )
    
                print("Last quarter started: %s" %last_period_start )
                print("Last quarter ended: %s" %last_period_end )
            
        if period == 'year':
            this_period_start = dt.date(year, 1, 1)
            this_period_end = today
    
            last_period_start = dt.date(year - 1, 1, 1)
            last_period_end = dt.date(year - 1, 12, 31)
            
            if verbose == True:
                print("This year starts: %s" %this_period_start )
                print("This year ends: %s" %this_period_end )
                
                print("Last year started: %s" %last_period_start )
                print("Last year ended: %s" %last_period_end )                
    except Exception as e:
        print(e)        
        sys.exit()

#Sets the wee

def set_week():
    try:
        global this_period_start
        global this_period_end
        global last_period_start
        global last_period_end
        
        this_period_start = today - dt.timedelta(dt.date.weekday(today))
        this_period_end = this_period_start + dt.timedelta(6)
        
        last_period_start = this_period_start - dt.timedelta(7)
        last_period_end = this_period_end - dt.timedelta(7)
        
    except Exception as e:
        print(e)        
        sys.exit()

#Sets the month
def set_month():
    next_month = today.replace(day=28) + dt.timedelta(days=4)
    
    global this_period_start
    global this_period_end
    global last_period_start
    global last_period_end
    
    this_period_start = dt.date(year, month, 1)
    this_period_end = next_month - dt.timedelta(next_month.day)
    
    last_period_end = this_period_start - dt.timedelta(1)
    last_period_start =  last_period_end - dt.timedelta(last_period_end.day -1)
    
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
    #try:
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
    #except Exception as e:
        #print(e)
        #sys.exit()
        

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
    #try:
        plot_base = dataframe.groupby([group_col])[group_col].agg(agg_method).sort_values(ascending = False)
        plot = plot_base.plot(kind=plot_kind, figsize = (10,10))
        fig = plot.get_figure()
        fig.savefig(name)
        plt.show()
    #except Exception as e:
        #print(e)
        #print('%tb')
    #else:
        #return fig
    
#This creates my graphs by time period
def period_figure_maker(df, datecolumn, xlab, ylab, legend1, legend2, title):
    
        x_count = (this_period_end - this_period_start)
        
        plt.xlim(0, x_count.days)
        
        #print(x_count.days)
        
    #try:        
        #print('Setting datetime indexes...')
        df['DateUnsubscribed'] = pandas.to_datetime(df[datecolumn])
        
        print(datecolumn)
        
        df['Day_Of_Year'] = df['DateUnsubscribed'].dt.dayofyear
        
        df = df.set_index(['DateUnsubscribed'])
        df.sort_index(inplace=True, ascending=True)
        
        #print("I've set the index!")
        
        #print('Creating time period slices...')
        
        this_period_data = df.loc[str(this_period_start):str(this_period_end)]
        #print("This period's data: ")
        #print(this_period_data.head())
        
        last_period_data = df.loc[str(last_period_start):str(last_period_end)]
        
        #print("Last period's data: ")
        #print(last_period_data.head())       
        
        this_period_count = (this_period_data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                           agg('count'))
        
        last_period_count = (last_period_data.groupby(['Day_Of_Year'])['Day_Of_Year'].
                           agg('count'))
        
        this_period_count.sort_index(inplace=True, ascending = True)
        last_period_count.sort_index(inplace=True, ascending = True)
        
        this_period_count.to_csv('this_period.csv')
        last_period_count.to_csv('last_period.csv')
        
        #print(this_period_count.head())
        #print(last_period_count.head())
        
        this_period_count = this_period_count.reset_index(drop=True)
        last_period_count = last_period_count.reset_index(drop=True)
        
        #last_period_average = df[[1]].mean()
        
        #print(last_period_average)
        
        #print(this_period_count.index)
        
        plt.plot(this_period_count, label = legend1)
        plt.plot(last_period_count, label = legend2)
        #plt.plot(last_period_average, label = 'Average')
        
        plt.xlabel = (xlab)
        plt.ylabel = (ylab)
        
        plt.legend(loc='upper right', shadow=True, fontsize='medium')
        
        plt.title(title)
        plt.show()
        #ax.plot(this_period_count)
        #ax.plot(last_period_count)
        #plot = fig.get_figure()
        
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

#This method creates the HTML to eventually output to a web page. 
def create_html(name, figure_dict, template):
    env = jinja2.Environment(loader=FileSystemLoad('.'))
    report_template = env.get_template(template)
    html_out = report_template.render(template_vars)

def churn_report(cnxn):
    churn_text = {}
    churn_graphs = {}
    
    #period_start = str(this_period_start)
    #period_end = str(this_period_end)
    
    #churn_text['Period Start'] = period_start
    #churn_text['Period End'] = period_end
    
    #churn_text.append('This covers list churn between %s and %s.') % (
            #start, end)
    
    tp_churn_subscribed_query = '''
    SELECT * from CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 2
    AND DateCreated <= '%s';
    ''' %this_period_end
    
    lp_churn_subscribed_query = '''
    SELECT * from CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 2
    AND DateCreated <= '%s';
    ''' %last_period_end    

    tp_churn_unsubscribed_query = """
    SELECT EmailSubscriptionStatusID, DateCreated, DateUnsubscribed 
    FROM CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 0
    AND DateCreated <= '%s'
    AND DateUnsubscribed >= '%s'
    AND DateUnsubscribed <= '%s'
    UNION
    /***Total who became neutral before the start and the end of the period***/
    SELECT EmailSubscriptionStatusID, DateCreated, DateModified 
    FROM CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 1
    AND DateCreated <= '%s'
    AND DateModified <= '%s'
    AND DateModified >= '%s';
    """ %(this_period_end, this_period_start, this_period_end, this_period_end, 
    this_period_end, this_period_start)
    
    lp_churn_unsubscribed_query = """
    SELECT EmailSubscriptionStatusID, DateCreated, DateUnsubscribed 
    FROM CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 0
    AND DateCreated <= '%s'
    AND DateUnsubscribed >= '%s'
    AND DateUnsubscribed <= '%s'
    UNION
    /***Total who became neutral before the start and the end of the period***/
    SELECT EmailSubscriptionStatusID, DateCreated, DateModified 
    FROM CRS_EmailSubscriptions
    WHERE EmailSubscriptionStatusID = 1
    AND DateCreated <= '%s'
    AND DateModified <= '%s'
    AND DateModified >= '%s';
    """ %(last_period_end, last_period_start, last_period_end, last_period_end, 
    last_period_end, last_period_start)
    
    tp_unsubscribed_df = frame_assembler(tp_churn_unsubscribed_query, 
                                         cnxn)
    
    tp_churn_total_df = frame_assembler(tp_churn_subscribed_query, cnxn, 
                                        'append', tp_unsubscribed_df)
    
    lp_unsubscribed_df = frame_assembler(lp_churn_unsubscribed_query,
                                         cnxn)
    
    lp_churn_total_df = frame_assembler(lp_churn_subscribed_query, 
                                        cnxn, 'append', lp_unsubscribed_df)
    
    all_churn_df = frame_assembler(lp_churn_unsubscribed_query, cnxn,
                                   'append', tp_unsubscribed_df)
    
    tp_churn = "{0:.3%}".format(
            len(tp_unsubscribed_df.index)/len(tp_churn_total_df)
            )
    
    lp_churn = "{0:.3%}".format(
            len(lp_unsubscribed_df.index)/len(lp_churn_total_df)
            )
    
    churn_text['This Period Churn'] = tp_churn
    churn_text['Last Period Churn'] = lp_churn
    
    churn_graph_for_period = period_figure_maker(all_churn_df,
                                                 'DateUnsubscribed', 
                                                 'Date Unsubscribed',
                                                 'Count of Records',
                                                 'Current Period', 'Previous Period', 'Churn')
    
    churn_graphs['Total Churn Over Time'] = churn_graph_for_period
    
    return churn_text, churn_graphs
    
    
#Main method, where the magic happens
def main():
    #set the quarter
    print('Setting dates...')
    set_time_period('month', True)
    
    ea_dw_cnxn = db_connect(cv.az_driver, 
                            cv.az_server,
                            cv.az_port,
                            cv.az_database,
                            cv.az_username,
                            cv.az_password)  
    
    #print('Getting data...')
    df = frame_assembler(form_and_revenue_query, ea_dw_cnxn)  
    
    #print(df.head())
    #print('Making figures...')

    churn_text, churn_graphs = churn_report(ea_dw_cnxn)
    print(churn_text['This Period Churn'])
    print(churn_text['Last Period Churn'])
    print(churn_graphs['Total Churn Over Time'])
    
    
    #period_figure_maker(df, 'DateCreated', 'Day of Quarter', 'Count', 'Current', 
                        #'Previous',
                    #'Form Transactions by Day - This Q vs. Prev')


    
    #Time to make some graphs!
    #figure_maker(top_five(df, 'platLong', 'dataframe'),'platLong', 'MS_Platform_Summary.png')
    #figure_maker(top_five(df, 'camplong', 'dataframe'), 'camplong', 'MS_Campaign_Summary.png')
    #figure_maker(top_five(df, 'creativelong', 'dataframe'), 'creativelong', 'MS_Creative_Summary.png')
    #figure_maker(top_five(df, 'mediumLong', 'dataframe'), 'mediumLong', 'MS_Medium_Summary.png')
    #figure_maker(top_five(df, 'progLong', 'dataframe'), 'progLong', 'MS_Program_Summary.png')

        
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
        
        #Email
            #Messages Sent
            #Recipients
            #Churn - Bounces & Unsubscribes
            
        #Email Lists
            #Count Active
            #Count Inactive
            #Total
            #Trend over Time
        #Forms
            #Total Submissions
            #New Contacts
        #Market Source
        
        #Revenue
            #Total Revenue
            #Total Donors
            #Total Gifts
            #Avg gift amt    
        
main()