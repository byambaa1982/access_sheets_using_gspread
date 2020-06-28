import gspread
import pandas as pd
import numpy as np
import json
import regex as re
import matplotlib
import matplotlib.pyplot as plt
import datetime
import os
# !pip install -U -q PyDrive
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
import gspread

gc = gspread.service_account(filename='/Users/enkhbat/fiverr/frank/google_dr/google_creds.json')

wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1-Uj9Z4L__1OVSMAtztiRxvaogGxZoxSSKDRLCRvdi00/edit#gid=0')

# print(wb.sheet1.get('A1'))

sheet = wb.worksheet('RawData')
meta_data=wb.worksheet('MetaData')
meta_death=wb.worksheet('MetaData-Deaths')
data = sheet.get_all_values()
df = pd.DataFrame(data)
df.columns = df.iloc[1]
df = df.iloc[2:]
df=df[['DIMENSION1', 'DIMENSION2', 'DIMENSION3', 'DIMENSION_TIME','FACT_FILTER', 'FACT_SUM']]
df["FACT_SUM"]=df["FACT_SUM"].map(lambda x:int(x))

df.to_csv('meta_data.csv', index=False)

def chart_it(x_data, y_data, x_label="", y_label="", title="", chart_type=""):
    _, ax = plt.subplots(figsize=(10, 6))
    # Draw bars, position them in the center of the tick mark on the x-axis
    if chart_type=="bar":
      ax.bar(x_data, y_data, color = '#539caf')
      # Draw error bars to show standard deviation, set ls to 'none'
      # to remove line between points
      ax.errorbar(x_data, y_data, color = '#297083', ls = 'none', lw = 2, capthick = 2)
      ax.set_ylabel(y_label, fontsize=16)
      ax.set_xlabel(x_label,fontsize=16)
      ax.set_title(title, fontsize=20, pad=30)
      plt.xticks(x_data, labels=x_data, rotation='vertical')
      plt.savefig(x_label+".png")
      plt.close('all')
    elif chart_type=="line":
      ax.plot(x_data, y_data, color = '#539caf')
      # Draw error bars to show standard deviation, set ls to 'none'
      # to remove line between points
      ax.errorbar(x_data, y_data, color = '#297083', ls = 'none', lw = 2, capthick = 2)
      ax.set_ylabel(y_label, fontsize=16)
      ax.set_xlabel(x_label,fontsize=16)
      ax.set_title(title, fontsize=20, pad=30)
      plt.xticks(x_data, labels=x_data, rotation='vertical')
      plt.savefig(x_label+".png")
      plt.close('all')
    elif chart_type=="scatter":
      ax.scatter(x_data, y_data, color = '#539caf')
      # Draw error bars to show standard deviation, set ls to 'none'
      # to remove line between points
      ax.errorbar(x_data, y_data, color = '#297083', ls = 'none', lw = 2, capthick = 2)
      ax.set_ylabel(y_label, fontsize=16)
      ax.set_xlabel(x_label,fontsize=16)
      ax.set_title(title, fontsize=20, pad=30)
      plt.xticks(x_data, labels=x_data, rotation='vertical')
      plt.savefig(x_label+".png")
      plt.close('all')


def iter_pd(df):
    for val in df.columns:
        yield val
    for row in df.to_numpy():
        for val in row:
            if pd.isna(val):
                yield ""
            else:
                yield val

def pandas_to_sheets(pandas_df, sheet, clear = True):
	# Updates all values in a workbook to match a pandas dataframe
	if clear:
		sheet.clear()
	(row, col) = pandas_df.shape
	cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
	for cell, val in zip(cells, iter_pd(pandas_df)):
		cell.value = val
	sheet.update_cells(cells)

my_dir=os.chdir('../google_dr/charts/')

meta_death=wb.worksheet('MetaData-Deaths')

def make_chart_for_countries_with_fact_filter(df,fact,sheet_name,url,chart_type):
	my_dir=os.chdir(url)
	data={}
	data["chartType"]=[]
	data["createdDate"]=[]
	data["fileName"]=[]
	data["validToDate"]=[]
	data["Questions"]=[]
	for country in countries:
		my_date=datetime.datetime.now()
		#--------- Change Questions here!!!-------------------
		question="What were the Covid "+fact+" in "+country+" 2020?"
		new=df.loc[(df["DIMENSION1"]==str(country)) & (df["FACT_FILTER"]==fact)]
		my_ser=new.groupby("DIMENSION_TIME")["FACT_SUM"].sum()
		df1=my_ser.to_frame()
		chart_it(df1.index, df1.FACT_SUM, x_label=country, y_label=fact, title=question,chart_type=chart_type )
		print(question)
		data["chartType"].append(chart_type)
		data["createdDate"].append(str(my_date))
		data["fileName"].append(country+".png")
		data["validToDate"].append("I do not know")
		data["Questions"].append(question)
	df=pd.DataFrame(data, columns=data.keys())
	# meta_data.update([df.columns.values.tolist()] + df.values.tolist())
	# meta_data.set_dataframe(df, 'A2')
	meta_data=wb.worksheet(sheet_name)
	pandas_to_sheets(df, meta_data)

url='/content/gdrive/My Drive/fiverr/frank/chart_deaths/'
fact='Deaths'
sheet_name='MetaData-Deaths'
#-- "line", "bar", "scatter"--
chart_type="line" 

# ---------       This is main function ------------------

make_chart_for_countries_with_fact_filter(df,fact,sheet_name,url,chart_type)

# 2. Auto-iterate using the query syntax
#    https://developers.google.com/drive/v2/web/search-parameters

# -------------  CHANGE FOLDER ID HERE !!!!!   ------------------

file_list = drive.ListFile({'q': "'14vuSdxd4ghvMeSRvJaz_kCa_UOC7gcPD' in parents"}).GetList()

data2={}
data2["fileName"]=[]
data2["id"]=[]
for f in file_list:
	# 3. Create & download by id.
	data2["fileName"].append(f['title'])
	data2["id"].append(f['id'])
df2=pd.DataFrame(data2, columns=data2.keys())

def meta_df(df,fact,sheet_name,chart_type):
	# my_dir=os.chdir(url)
	data={}
	data["chartType"]=[]
	data["createdDate"]=[]
	data["fileName"]=[]
	data["validToDate"]=[]
	data["Questions"]=[]
	for country in countries:
		my_date=datetime.datetime.now()
		#--------- Change Questions here!!!-------------------
		question="What were the Covid "+fact+" in "+country+" 2020?"
		new=df.loc[(df["DIMENSION1"]==str(country)) & (df["FACT_FILTER"]==fact)]
		my_ser=new.groupby("DIMENSION_TIME")["FACT_SUM"].sum()
		df1=my_ser.to_frame()
		# chart_it(df1.index, df1.FACT_SUM, x_label=country, y_label=fact, title=question,chart_type=chart_type )
		# print(question)
		data["chartType"].append(chart_type)
		data["createdDate"].append(str(my_date))
		data["fileName"].append(country+".png")
		data["validToDate"].append("I do not know")
		data["Questions"].append(question)
	df=pd.DataFrame(data, columns=data.keys())
	return df

fact='Deaths'
sheet_name='MetaData-Deaths'
#-- "line", "bar", "scatter"--
chart_type="line" 
df1=meta_df(df,fact,sheet_name,chart_type)

result = pd.merge(df1, df2, on='fileName')
result["links"]=result["id"].map(lambda x: "https://drive.google.com/file/d/"+str(x)+"/view?usp=drivesdk")
meta_data=wb.worksheet(sheet_name)
pandas_to_sheets(result, meta_data)




