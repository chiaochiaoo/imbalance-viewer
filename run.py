import os
from csv import reader
import pandas as pd
import json

#read csv

d = {}

def read_file(etf_name,file):
	global d 
	a=pd.read_csv(file,skiprows=0,header=1)

	for index, row in a.iterrows():
	#iterate through, add the 
		if row["Symbol"] not in d:
			d[row["Symbol"]] = {}

		d[row["Symbol"]]["etf"] = etf_name
		d[row["Symbol"]]["weight"] = int(float(row["Weight"][:-1])*100)


	#print(int(float(row["Weight"][:-1])*100))

directory = os.fsencode("./db/")
	
for file in os.listdir(directory):
	filename = os.fsdecode(file)
	if filename.endswith(".asm") or filename.endswith(".csv"): 
		print(filename)

		read_file(str.upper(filename[:-4]),"./db/"+filename)
		a=pd.read_csv("./db/"+filename, 'r')
		# with open("./db/"+filename, 'r') as read_obj:
		# # pass the file object to reader() to get the reader object
		# 	csv_reader = reader(read_obj)
		# # Iterate over each row in the csv using reader object
		# 	for row in csv_reader:
		# 	# row variable is a list that represents a row in csv
		# 		print(row)
	else:
		continue
with open('data.json', 'w') as f:
    json.dump(d, f,indent=4)
