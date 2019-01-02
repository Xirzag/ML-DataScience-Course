import numpy as np
import pandas as pd

samples_perc = 0.2

folders = ['']

for folder in folders:
    for number in range(54, 55):
        print('Adding', number)
        df = pd.read_csv('C:/data/' + folder + '/Kickstarter' + '%03.1i' % number + '.csv').sample(n=samples_perc)
        df.to_csv('bigkickstarter.csv', header=False, mode='a')



