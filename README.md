# Pallavi Tribe leader

Data extraction and visualization:

Technology used :
       python:[
           Pandas,
           openpyxl,
           pymysql,
           sqlalchemy,
           matplotlib,
           seaborn ]
           
            Mysql Database:

             AWS: if you want to run this on instance(you can also use local machine (windows or linux)
             [Red Hat ec2 instance(used by the Team)] 

Packages and Softwares that are required to be installed:
       1.  Virtual environment setup [In any IDE - Pycharm Professional(recommended)]
            if your are using aws linux instance  or any linux kernel in your machine just install python.
            [for centos use- dnf install python3.8 ,
             for ubuntu use- apt install python3.8]
             you can install any version of python but python 3.8 is recommended .

        2.  Pandas: 
               [for IDE use- pip install pandas  ,
                for AWS instance or centos use- pip3 install pandas (if python3 is installed),
                or use -dnf install pip amd then pip install pandas]
 
        3.Openpyxl:
            [for IDE use- pip install openpyxl,
             for AWS linux instance and for any linux instance use- pip install openpyxl (if pip is installed using dnf install pip or apt install pip)]

        4. PyMysql:
            [ for IDE use- pip install pymysql,
            for AWS linux instance and for any linux instance use-pip install pymysql (if pip is installed using dnf install pip or apt install pip)]

         5. Sqlalchemy:
              [ for IDE use- pip install sqlalchemy,
               for AWS linux instance and for any linux instance use-pip install sqlalchemy (if pip is installed using dnf install pip or apt install pip)]
 
        6. Matplotlib:
             [ for IDE use- pip install matplotlib,
             for AWS linux instance and for any linux instance use-pip install matplotlib (if pip is installed using dnf install pip or apt install pip)]

        7.  Seaborn:
             [ for IDE use- pip install seaborn,
             for AWS linux instance and for any linux instance use-pip install seaborn (if pip is installed using dnf install pip or apt install pip)]

        8. Mysql database:
            [for windows you can install it from-https://dev.mysql.com/downloads/installer/,
             for AWS Centos linux instance or Centos  linux kernel in your machine -
               [1. sudo dnf install mysql-server
                2. sudo systemctl start mysqld.service
                3. sudo systemctl status mysqld
                4. sudo systemctl enable mysqld
                5. sudo systemctl enable mysqld
                6. mysql -p ]


  Code:
             
              import pandas as pd
              import matplotlib.pyplot as plt
              import seaborn as sns
              from PyPDF2 import PdfFileMerger
              from sqlalchemy import create_engine
              from sqlalchemy import event
              from pandas.io import sql
              import pymysql as py
              import time
              from datetime import date
              import os
              import numpy as np
              import glob
              import logging
              sd = date.today()
              date = sd.strftime('%Y%m%d')
              host = '127.0.0.1'
              user = 'root'
              password = ' '
              database = 'data'
              rega = 'Regression Model'
              empa = 'Empirical Model'
              regs = 'Reg'
              emps = 'Em'

              csv1 = 'E:/dataset/data/data1.csv'

              quart = []
              year = []
              reg = []
              regmax = []
              regmin = []
              emp = []
              empmax = []
              empmin = []

              tick = []

              mod1 = []
              mod2 = []

              type = []

              cola = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M',
                      14: 'N', 15: '0', 16: 'P', 17: 'Q',18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z'}

              engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=host, db=database, user=user, pw=password))
              sql.execute("drop table if exists analystdata", engine)
              con = py.connect(host='127.0.0.1', port=3306, user='root', passwd=' ', db='data')
              cur=con.cursor()
              path5='E:/dataset/logs'
              fil=glob.glob(path5+"/*.log")
              for file in fil:
                  if "time.log" in file:
                      os.remove(file)
              logging.basicConfig(filename=path5+'/time.log',format='%(levelname)s:%(message)s',level=logging.INFO)
              h = "Reg"
              u = 'Em'
              path4 = 'E:/dataset/vdata/'
              path3 = 'E:/dataset/plots/'
              path2 = 'E:/dataset/data/'
              path1 = r'E:\ETL\Data'
              start_l = time.time()
              filea = glob.glob(path2 + "/*.xlsx")
              for fil in filea:
                  if "Model_" + date + ".xlsx" in fil:
                      os.remove(fil)
              all_files = glob.glob(path1 + "/*.xlsx")
              for filename in all_files:
                  start1 = time.time()
                  files = pd.ExcelFile(filename, engine="openpyxl")
                  sheets = files.sheet_names
                  for i in sheets:
                      if regs in i:
                          mod1.append(i)
                      if emps in i:
                          mod2.append(i)


                  raw1 = []
                  col1 = []
                  row2 = []
                  col2 = []

                  if len(mod1)==0 and len(mod2)==0:
                      logging.info(filename+"dont have any regression and empirical model")
                  else:
                    try:
                      def v(na):
                          rd = pd.read_excel(filename, sheet_name=mod1[na], engine="openpyxl")
                          val1 = np.array(rd)
                          cal1 = np.argwhere(val1 == "Min")
                          row = cal1[0, 0]
                          raw1.append(row)
                          col = cal1[0, 1]
                          col1.append(col)
                    except IndexError:
                        logging.info(filename + "dont have required sheet")
                    try:
                      def v2(na):
                          rd = pd.read_excel(filename, sheet_name=mod2[na], engine="openpyxl")
                          val1 = np.array(rd)
                          cal1 = np.argwhere(val1 == "Min")
                          row = cal1[0, 0]
                          row2.append(row)
                          col = cal1[0, 1]
                          col2.append(col)


                      ji = range(0, len(mod1))
                      jh = range(0, len(mod2))
                      hu = map(v, ji)
                      uh = map(v2, jh)
                      list(hu)
                      list(uh)
                    except IndexError:
                        logging.info(filename + "dont have required sheet")
                    try:
                      def qy():
                          ambd2 = pd.read_excel(filename, sheet_name=mod1[0], header=0, usecols=cola.get(col1[0] - 13), dtype=str,
                                                engine="openpyxl")
                          ambd2.to_csv(csv1, index=False, header=True)
                          ambd2 = pd.read_csv(csv1, engine='c')
                          for i in ambd2.columns:
                              amb34 = ambd2[i][raw1[0] - 2]
                              d1 = amb34[:1]
                              d1a = amb34[1:2]
                              d1b = d1a + d1
                              d2 = amb34[2:4]
                              d3 = '20' + d2
                              if len(mod1) == len(mod2) or len(mod1) > len(mod2):
                                  for i in range(len(mod1)):
                                      quart.append(d1b)
                                      year.append(d3)
                              elif len(mod2) > len(mod1):
                                  for i in range(len(mod2)):
                                      quart.append(d1b)
                                      year.append(d3)
                              ds = filename
                              ds1 = ds[12:]
                              end1 = time.time()
                              t = end1 - start1
                              logging.info("time taken to extract quarter from " + ds1 + " is {0}".format(t))
                              os.remove(csv1)
                      qy()
                    except IndexError:
                        logging.info(filename + "dont have required sheet")
                    try:
                      def re(na, nc):
                          start3 = time.time()
                          ambd2 = pd.read_excel(filename, sheet_name=mod1[na], header=0, usecols=cola.get(col1[nc] + 2), dtype=str,
                                                engine="openpyxl")
                          ambd2.to_csv(csv1, index=False, header=True)
                          ambd2 = pd.read_csv(csv1, engine='c')
                          for i in ambd2.columns:
                              dbe1 = round(float(ambd2[i][raw1[nc] - 2]), 1)
                              abe2 = round(float(ambd2[i][raw1[nc] - 1]), 1)
                              abe3 = round(float(ambd2[i][raw1[nc]]), 1)
                              reg.append(dbe1)
                              regmax.append(abe2)
                              regmin.append(abe3)
                              ds = filename
                              ds1 = ds[12:]
                              ds = filename[12:16]
                              end3 = time.time()
                              tick.append(ds)
                              t = end3 - start3
                              logging.info(
                                  "time taken to extract Forecast w/o SA Actual,Forecast w/o SA Max,Forecast w/o SA Min from " + ds1 + " is {0}".format(
                                      t))
                              os.remove(csv1)


                      if len(mod1) > len(mod2):
                          for i in mod1:
                              fg = str(i)
                              if len(rega) < len(fg):
                                  emp.append('NULL')
                                  empmax.append('NULL')
                                  empmin.append('NULL')
                      gh = range(0, len(raw1))
                      fg = range(0, len(mod1))
                      fun = map(re, fg, gh)
                      list(fun)
                    except IndexError:
                        logging.info(filename + "dont have required sheet")
                    try:
                      def ty():
                          if len(mod1) == len(mod2) or len(mod1) > len(mod2):
                              for i in mod1:
                                  fg = str(i)
                                  if rega in fg:
                                      if len(rega) < len(i):
                                          ds = i.split("-")
                                          dc = ds[1:]
                                          cd = ''.join(dc)
                                          type.append(cd)
                                      else:
                                          type.append("NULL")

                          elif len(mod2) > len(mod1):
                              for i in mod2:
                                  fg = str(i)
                                  if empa in fg:
                                      if len(empa) < len(i):
                                          ds = i.split("-")
                                          dc = ds[1:]
                                          cd = ''.join(dc)
                                          type.append(cd)
                                      else:
                                          type.append("NULL")


                      ty()
                    except IndexError:
                        logging.info(filename + "dont have required sheet")
                    try:
                      def em(na, nc):
                          start4 = time.time()
                          ambd4 = pd.read_excel(filename, sheet_name=mod2[na], usecols=cola.get(col2[nc] + 2), engine="openpyxl")
                          ambd4.to_csv(csv1, index=False, header=True)
                          ambd4 = pd.read_csv(csv1, engine='c')
                          for i in ambd4.columns:
                              dbe2 = round(float(ambd4[i][row2[nc] - 2]), 1)
                              abe3 = round(float(ambd4[i][row2[nc] - 1]), 1)
                              abe4 = round(float(ambd4[i][row2[nc]]), 1)
                              ds = filename
                              ds1 = ds[12:]
                              emp.append(dbe2)
                              empmax.append(abe3)
                              empmin.append(abe4)
                              end4 = time.time()
                              d = end4 - start4
                              logging.info(
                                  "time taken to extract Estimated Total Sold,Estimated Sold Max,Estimated Sold Min from " + ds1 + " is {}".format(
                                      d))
                              os.remove(csv1)


                      if len(mod1) < len(mod2):
                          for i in mod1:
                              fg = str(i)
                              if len(empa) < len(fg):
                                  ds = filename
                                  ds1 = ds[12:]
                                  ds = filename[12:16]
                                  tick.append(ds)
                                  reg.append('NULL')
                                  regmax.append('NULL')
                                  regmin.append('NULL')
                      gd = range(0, len(col2))
                      sg = range(0, len(mod2))
                      sun = map(em, sg, gd)
                      list(sun)
                      raw1.clear()
                      col1.clear()
                      row2.clear()
                      col2.clear()
                      mod1.clear()
                      mod2.clear()
                    except IndexError:
                      logging.info(filename + "dont have required sheet")

              datas = {'Date': [sd] * len(tick), 'Ticker': tick, 'Type': type, 'Quarter': quart, 'Year': year,
                       'Estimated Total Sold': emp, 'Estimated Sold Min': empmin,

                       'Estimated Sold Max': empmax, 'Forecast w/o SA Actual': reg, 'Forecast w/o SA Max': regmax,
                       'Forecast w/o SA Min': regmin}

              dat23 = pd.DataFrame(datas, columns=['Date', 'Ticker', 'Type', 'Quarter', 'Year', 'Estimated Total Sold',
                                                   'Estimated Sold Max', 'Estimated Sold Min',
                                                   'Forecast w/o SA Actual', 'Forecast w/o SA Max', 'Forecast w/o SA Min'])

              dat23.to_excel(path2 + 'Model_' + date + ".xlsx", index=False)


              @event.listens_for(engine, 'before_cursor_execute')
              def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                  if executemany:
                      cursor.fast_executemany = True
                      cursor.commit()
              dat23.to_sql('analystdata',engine,method='multi',index=False)

              endl = time.time()
              fin = endl - start_l
              logging.info("time taken to post extracted data into mysql {0}".format(fin))
              startp = time.time()
              merger = PdfFileMerger(strict=False)
              def plots(t1, t2, y1, e1, e2, e3, f1, f2, f3):
                  tick1 = t1
                  type = t2
                  year1 = y1
                  es1 = e1
                  es1_max = e2
                  es1_min = e3
                  fs1 = f1
                  fs1_max = f2
                  fs1_min = f3
                  sns.set_theme(style="darkgrid")
                  name = ['Estimated Total Sold', 'Estimated Sold Max', 'Estimated Sold Min', 'Forecast w/o SA Actual',
                          'Forecast w/o SA Max', 'Forecast w/o SA Min']
                  value = [es1, es1_max, es1_min, fs1, fs1_max, fs1_min]
                  name1 = ['Estimated Total Sold', 'Estimated Sold Max', 'Estimated Sold Min']
                  name2 = ['Forecast w/o SA Actual', 'Forecast w/o SA Max', 'Forecast w/o SA Min']
                  value1 = [es1, es1_max, es1_min]
                  value2 = [fs1, fs1_max, fs1_min]
                  fig1 = plt.figure(figsize=(20, 10))
                  plt.bar(name1, value1, color='darkblue', width=0.3)
                  plt.bar(name2, value2, color='lightblue', width=0.3)
                  plt.xlabel('Estimated Sold and Forecast w/o SA for type(' + type + ')')
                  plt.ylabel('values')
                  plt.title('Estimated solds and Forecast w/o SA Of ' + tick1 + '-Year(' + year1 + ')')
                  for i in range(len(name)):
                      plt.text(i, value[i], value[i], ha='center', color='gray', size='large', variant='small-caps')
                  fig1.savefig(path3 + tick1 + "_type(" + type + ").pdf", format='pdf')

              for filename in filea:
                      yt = pd.read_excel(path2+"Model_"+date+".xlsx", usecols="B,C,E:K", engine="openpyxl")
                      df = np.array(yt)
                      for j in range(0, len(df)):
                          plots(str(df[j, 0]), str(df[j, 1]), str(df[j, 2]), df[j, 3], df[j, 4], df[j, 5], df[j, 6], df[j, 7],
                                df[j, 8])
              pdf = glob.glob(path3 + "/*.pdf")
              for fileas in pdf:
                  merger.append(fileas)
              merger.write(path4 + "Graphs.pdf")
              merger.close()
              for fileas in pdf:
                  os.remove(fileas)

              endp=time.time()
              fin2=endp-startp
              logging.info("time taken to create graphical representation of output file is {0}".format(fin2))
              col1 = ['Date', 'FacilityType', 'BedSize', 'Region', 'Manufacturer', 'Ticker', 'Group',
                      'Therapy', 'Anatomy', 'SubAnatomy', 'ProductCategory', 'Quantity', 'AvgPrice',
                      'TotalSpend']
              start_f = time.time()
              sql.execute("drop table if exists data", engine)
              all_files = glob.glob(path1 + "/*.xlsx")
              for filename in all_files:
                  start = time.time()
                  fs = pd.read_excel(filename, sheet_name='Data', dtype=str,skiprows=range(2,10000000), header=0, engine="openpyxl").columns
                  d = list(fs)
                  col = []
                  for i in d:
                      for j in col1:
                          if i == j:
                              col.append(i)
                  if 'Date' in col:
                      start=time.time()
                      fg = pd.read_excel(filename, sheet_name='Data',skiprows=range(2,10000),usecols=col,na_filter=False ,engine="openpyxl")
                      fg.to_csv(csv1,index=False,header=True)
                      cg=pd.read_csv(csv1,engine='c')
                      df = np.array(cg)
                      tf = cg['Date'].max()
                      vg = np.argwhere(df == tf)
                      vh = df[vg[:, 0]]
                      cv=pd.DataFrame(vh,columns=col,index=None)
                      @event.listens_for(engine, 'before_cursor_execute')
                      def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                          if executemany:
                              cursor.fast_executemany = True
                              cursor.commit()
                      cv.to_sql('data', engine,method='multi',chunksize=10000,if_exists='append', index=False)
                      end=time.time()
                      f=end-start
                      logging.info("time taken to post"+filename+"into mysql is {0}".format(f))
                      os.remove(csv1)
              end_f=time.time()
              fna=end_f-start_f
              logging.info("time taken to post the data in the database {0}".format(fna))
