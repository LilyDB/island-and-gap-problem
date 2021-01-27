

import pandas as pd
import numpy as np
import os

filename =  r'C:\Users\phol\Desktop\data2.2.csv'
output = []
with open(filename) as f:

    prev_id = None
    tmpline = ''
    for line in f:
        line = line.strip()
        (Company, stockout_start, stockout_end) = line.rsplit(';', 3)

        # append to output when new ID changes or 
        # date1 > previous date2 (start new range of dates)
        if prev_id != Company or (stockout_start > tmpline[2] and stockout_end<tmpline[2]):
            if tmpline:
               output.append(';'.join(tmpline))
            tmpline = [Company, stockout_start, stockout_end]

        # override end date if larger
        elif (stockout_start < tmpline[2] ):
            tmpline[2] = stockout_end
            
        elif (stockout_start > tmpline[2] and stockout_end> tmpline[2]):
            tmpline[2] ==0
            
        prev_id = Company

    # take care last line
    tmpline = ';'.join(tmpline)
    if tmpline != output[-1]:
        output.append(tmpline)
        output = pd.DataFrame(output,columns=['df'])
        
        output[['Company',
                'stockout_start',
                'stockout_end']] = output['df'].str.split(";",expand=True,)
        output = output.drop('df',axis=1)
        output['stockout_start'] = pd.to_datetime(output['stockout_start'],
                                                  format = '%d-%m-%Y')
        output['stockout_end'] = pd.to_datetime(output['stockout_end'],
                                                format = '%d-%m-%Y')
        output['num_stockout_days'] = (output['stockout_end']-output['stockout_start'])\
                                /np.timedelta64(1,'D')+1
      
        output = output.groupby('Company')\
                        .agg({'num_stockout_days':'sum'})
        output['free_stockout_days'] = (366-output['num_stockout_days'] )/366
        output.sort_values(by='num_stockout_days', 
                           ascending=False)

fileoutput = r'C:\Users\phol\Desktop\Output2.3.csv'      
if os.path.exists(fileoutput):
    os.remove(file)
    output.to_csv(file,index=False)
    print('number of records: ' + str(len(output)))
    print("done")
    output.to_csv(file)
