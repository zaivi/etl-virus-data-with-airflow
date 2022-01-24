import csv
from datetime import timedelta, datetime

class PreprocessJob():
    # Initialize attributes
    def __init__(self, file_name):
        self.file_name = file_name
        self.data = None
        self.header = None
        self.read_file()

    # Write result to csv file
    def write_csv(self, filename):
        with open(filename, 'w') as csvfile: 
            csvwriter = csv.writer(csvfile, delimiter='\t') 
            csvwriter.writerow(self.header)
            csvwriter.writerows(self.data)
    
    # Read raw data from file
    def read_file(self):
        file = open(f'data/{self.file_name}', 'r')
        csvreader = csv.reader(file, delimiter='\t')
        header = next(csvreader)
        rows = [header]
        for row in csvreader:
            rows.append(row)
        self.data = rows
        file.close()

    # Main function for preprocessing raw data
    def preprocess(self):
        print('--------------------- Running Preprocessing Job')
        for i in range(len(self.data)):
            for j in range(len(self.data[i])):
                if self.data[i][j] == '' or self.data[i][j] == 'N/A':
                    self.data[i][j] = ''
                else:
                    if self.data[i][j][0] == '+':
                            self.data[i][j] = self.data[i][j][1:]
                    if self.data[i][j][0].isdigit() and i != 1 and i != len(self.data) - 1:
                        self.data[i][j] = int(''.join(self.data[i][j].split(',')))
        self.header = self.data[0]
        self.data = self.data[2: len(self.data) - 1]
        self.data = [row[:-1] for row in self.data]
        self.write_csv(f'preprocess-data/preprocessed-{self.file_name}')
        print('--------------------- Done Preprocessing Job')
        

def run_etl_preprocess():
    file_name = datetime.today().strftime('%d-%m-%Y')
    preprocess_obj = PreprocessJob(f'{file_name}.csv')
    preprocess_obj.preprocess()