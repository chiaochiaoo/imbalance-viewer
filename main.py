import tkinter as tk   
from tkinter import ttk                  
import csv
import json
import time
import multiprocessing
import threading

NEW_ETF ="New ETF"
UPDATE ="Update"
GREEN = "#97FEA8"
DEFAULT = "#d9d9d9"
LIGHTYELLOW = "#fef0b8"
YELLOW =  "#ECF57C"
VERYLIGHTGREEN = "#ecf8e1"
LIGHTGREEN = "#97FEA8"
STRONGGREEN = "#3DFC68"
STRONGRED = "#FC433D"
DEEPGREEN = "#059a12"
PINK = "#FB7356"

def find_between(data, first, last):
	try:
		start = data.index(first) + len(first)
		end = data.index(last, start)
		return data[start:end]
	except ValueError:
		return data
def timestamp_seconds(s):

	p = s.split(":")
	try:
		x = int(p[0])*3600+int(p[1])*60+int(p[2])
		return x
	except Exception as e:
		print("Timestamp conversion error:",e)
		return 0
def timestamp(s):
	p = s.split(":")
	try:
		x = int(p[0])*60+int(p[1])
		return x
	except Exception as e:
		print("Timestamp conversion error:",e)
		return 0


class processor:

	def __init__(self,send_pipe):

		with open("data.json") as f:
			self.data = json.load(f)

		self.etfs_names = self.data["all_etfs"]
		self.symbols= self.data.keys()
		self.sendpipe = send_pipe
		#print(self.etfs,self.symbols)

		"""For each etf, create a data object """
		self.etfs = {}
		for i in self.etfs_names:
			self.add_new_etf(i,self.sendpipe)

		good = threading.Thread(target=self.test_mode, daemon=True)
		good.start()

		#read the json file.

	def add_new_etf(self,etf,send_pipe):
		#print(etf)
		self.etfs[etf] = ETF(etf,send_pipe)
		self.sendpipe.send([NEW_ETF,etf])

	def test_mode(self):

		#send,,, when?
		with open('imbalance514.csv') as csv_file:
			csv_reader = csv.reader(csv_file, delimiter=',')
			line_count = 1
			for row in csv_reader:
				row = row[0]
				Symbol = find_between(row, "Symbol=", ",")
				symbol = Symbol[:-3]
				time_ = find_between(row, "MarketTime=", ",")[:-4]
				ts=timestamp_seconds(time_)
				### ONLY PROCEED IF IT IS IN THE SYMBOL LIST ###57000
				if symbol in self.symbols and ts>=57000:
					market = symbol[-2:]
					
					side = find_between(row, "Side=", ",")
					volume =  int(find_between(row, "Volume=", ","))
					source = find_between(row, "Source=", ",")

					data = self.data[symbol]
					etf = data["etf"]
					weight = data["weight"]

					self.etfs[etf].new_imbalance(side,volume,weight,time_,ts)

					#time.sleep(0.00001)


class ETF:
	def __init__(self,name,pipe):
		self.name = name
		self.data = {}
		self.data["buy"] = 0
		self.data["sell"] = 0
		self.data["Δbuy"] = 0
		self.data["Δsell"] = 0
		self.data["B/S"] = 0
		self.data["ΔB/S"] = 0


		self.time = ""
		self.ts = 0
		#self.last_ts = 0
		self.pipe = pipe

		self.buy_1min_trailing = []
		self.sell_1min_trailing = []
		self.bsratio_1min_trailing = []

	def new_imbalance(self,side,quantity,weight,time_,ts):

		if side =="B":
			self.data["buy"]+=quantity*weight
		elif side =="S":
			self.data["sell"]+=quantity*weight

		if ts -self.ts >=5:
			self.calc_delta(time_,ts)

	"""RUN EVERY 5 SECONDS"""
	def calc_delta(self,time_,ts):

		time.sleep(0.1)
		self.time = time_
		self.ts = ts 

		if self.data["buy"]>self.data["sell"]:
			self.data["B/S"] = round((self.data["buy"]/(self.data["sell"]+1)),2)
		else:
			self.data["B/S"] = round(-(self.data["sell"]/(self.data["buy"]+1)),2)

		self.bsratio_1min_trailing.append(self.data["B/S"])
		self.buy_1min_trailing.append(self.data["buy"])
		self.sell_1min_trailing.append(self.data["sell"])

		if len(self.buy_1min_trailing)>=13:
			self.buy_1min_trailing.pop(0)
		if len(self.sell_1min_trailing)>=13:
			self.sell_1min_trailing.pop(0)
		if len(self.bsratio_1min_trailing)>=13:
			self.bsratio_1min_trailing.pop(0)

		if len(self.buy_1min_trailing)>7:
			self.data["Δbuy"] = round((self.data["buy"] - self.buy_1min_trailing[-7])/self.buy_1min_trailing[-7],2)

			#print(self.name,self.data["buy"],self.buy_1min_trailing)

		if len(self.sell_1min_trailing)>7:
			self.data["Δsell"] = round((self.data["sell"] - self.sell_1min_trailing[-7])/self.sell_1min_trailing[-7],2)

		if len(self.bsratio_1min_trailing)>7:
			self.data["ΔB/S"] = round((self.data["B/S"] - self.bsratio_1min_trailing[-7])/self.bsratio_1min_trailing[-7],2)

		self.pipe.send([UPDATE,self.name,self.data,self.time])
		#print(self.name,self.data["buy"],self.data["Δbuy"],self.data["sell"],self.data["Δsell"],self.data["B/S"],self.delta_bsratio,self.ts)

### SYNC EVERY SECOND. 
class UI:
	def __init__(self,root,rec_pipe):

		self.root = root
		self.rec_pipe = rec_pipe

		self.label_count = 2

		self.etfs = {}
		self.etfs_labels = {}

		self.init_pannel()

		good = threading.Thread(target=self.update, daemon=True)
		good.start()

	def init_pannel(self):
		self.labels = {"ETF":10,\
						"Buy":10,\
						"ΔBuy":10,\
						"Sell":10,\
						"ΔSell":10,\
						"B/S":10,\
						"ΔB/S":10,\
						}

		self.width = list(self.labels.values())

		self.hq = ttk.LabelFrame(self.root,text="Main") 
		self.hq.place(x=10,rely=0.01,relheight=0.2,relwidth=0.95)

		
		tk.Label(self.hq, text="time:",width=5,height=5).grid(row=1, column=1)
		self.time = tk.StringVar(value="")
		tk.Label(self.hq, textvariable=self.time,width=10,height=5).grid(row=1, column=2)

		self.bg = ttk.LabelFrame(self.root,text="") 
		self.bg.place(x=10,rely=0.2,relheight=0.95,relwidth=0.95)

		self.recreate_labels()


	def recreate_labels(self):

		l = list(self.labels.keys())
		w = list(self.labels.values())

		for i in range(len(l)): #Rows
			self.b = tk.Button(self.bg, text=l[i],width=w[i],height=2)#,command=self.rank
			self.b.configure(activebackground="#f9f9f9")
			self.b.configure(activeforeground="black")
			self.b.configure(background="#d9d9d9")
			self.b.configure(disabledforeground="#a3a3a3")
			self.b.configure(relief="ridge")
			self.b.configure(foreground="#000000")
			self.b.configure(highlightbackground="#d9d9d9")
			self.b.configure(highlightcolor="black")
			self.b.grid(row=1, column=i)

	def new_etf(self,etf):
		
		"""init data, create labels."""
		self.etfs[etf] = {}
		self.etfs_labels[etf] = {}

		data = self.etfs[etf]

		keys = ["name","buy","Δbuy","sell","Δsell","B/S","ΔB/S"]

		for i in keys:
			data[i] = tk.StringVar()

		data["name"] = tk.StringVar(value=etf)

		l = self.label_count

		for i in range(len(keys)): #Rows
			self.etfs_labels[etf][keys[i]] = tk.Label(self.bg, textvariable=data[keys[i]],width=11,height=2)#,command=self.rank
			self.etfs_labels[etf][keys[i]].configure(activebackground="#f9f9f9")
			self.etfs_labels[etf][keys[i]].configure(activeforeground="black")
			self.etfs_labels[etf][keys[i]].configure(background="#d9d9d9")
			self.etfs_labels[etf][keys[i]].configure(disabledforeground="#a3a3a3")
			self.etfs_labels[etf][keys[i]].configure(relief="ridge")
			self.etfs_labels[etf][keys[i]].configure(foreground="#000000")
			self.etfs_labels[etf][keys[i]].configure(highlightbackground="#d9d9d9")
			self.etfs_labels[etf][keys[i]].configure(highlightcolor="black")
			self.etfs_labels[etf][keys[i]].grid(row=l, column=i)

		self.label_count+=1

	def update_etf(self,etf,data,time_):
		
		### DATA IS A DIC### 
		#data = self.etfs[etf]

		self.time.set(time_)
		for key,item in data.items():
			if key== "buy" or key=="sell":
				self.etfs[etf][key].set(str(round(item/1000000000,2))+"m")
			elif key== "Δbuy" or key== "Δsell":
				if item >1:
					self.etfs_labels[etf][key]["background"] = YELLOW
				if item >4:
					self.etfs_labels[etf][key]["background"] = "red"
				else:
					self.etfs_labels[etf][key]["background"] = DEFAULT
				self.etfs[etf][key].set(item)
			elif key== "B/S":
				if item <-4:
					self.etfs_labels[etf][key]["background"] = PINK
				elif item >4:
					self.etfs_labels[etf][key]["background"] = LIGHTGREEN
				else:
					self.etfs_labels[etf][key]["background"] = DEFAULT

				self.etfs[etf][key].set(item)

			elif key== "ΔB/S":
				if abs(item) >0.5:
					self.etfs_labels[etf][key]["background"] = "red"
				else:
					self.etfs_labels[etf][key]["background"] = DEFAULT

				self.etfs[etf][key].set(item)
			else:
				self.etfs[etf][key].set(item)


	def update(self):

		while True:

			info = self.rec_pipe.recv()

			cmd = info[0]

			if cmd == NEW_ETF:
				self.new_etf(info[1])

			if cmd ==UPDATE:
				etf = info[1]
				data=info[2]
				ts=info[3]
				self.update_etf(etf,data,ts)


if __name__ == '__main__':

	multiprocessing.freeze_support()
	send_pipe, receive_pipe = multiprocessing.Pipe()

	root = tk.Tk() 
	root.title("Imbalance viewer") 
	root.geometry("900x500")

	a= processor(send_pipe)
	ui = UI(root,receive_pipe)
	# root.minsize(1600, 1000)
	# root.maxsize(1800, 1200)
	root.mainloop()