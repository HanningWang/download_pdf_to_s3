import os

# The file contains info about all urls, format {eid},{doi},{url}
INPUT_FILE = 'input_file.csv'
# The dir contains files of urls to be removed
TARGET_DIR = 'result/success/'

OUTPUT_FILE = 'output_file.txt'

url_dict = dict()
with open(INPUT_FILE, 'r') as file:
    for line in file:
        url = line.strip().split(",")[2].split(";")[0]
        print(type(url))
        print(type(line))
        url_dict[url] = line

for file in os.listdir(TARGET_DIR):
    file_path = os.path.join(TARGET_DIR, file)
    with open(file_path, 'r') as file:
        for line in file:
            url = line.strip()
            if url in url_dict:
                del url_dict[url]

with open(OUTPUT_FILE, 'w') as file:
    for key,value in url_dict.items():
        file.write(value)