import argparse
import os
import gzip
import pandas

PROJECTS_PATH = r'/work/sequence_data_store/'
SPLIT = '/'



def find_all_repertoires(project_name):
    found_files_paths = []
    project_path = os.path.join(PROJECTS_PATH, project_name, 'adc_annotated')
    for root, dirs, files in os.walk(project_path):
        for file in files:
            if ".tsv" in file:
                found_files_paths.append(os.path.join(root,file))

    return found_files_paths

def create_preprocessed_structure(project_name ,reperoires_paths):
    pre_processed = os.path.join(PROJECTS_PATH, project_name, 'runs', 'current', 'pre_processed')
    if not os.path.isdir(pre_processed):
        os.makedirs(pre_processed)

    for path in reperoires_paths:
        subdirs = path.split(SPLIT)[-4:-1]
        file_name = path.split(SPLIT)[-1]
        rep_path = os.path.join(pre_processed, *subdirs,  'reads', 'heavy')

        if not os.path.isdir(rep_path):
            os.makedirs(rep_path)

        extract_zip_file(path, rep_path, file_name)

        
def extract_zip_file(file_path, file_dest_path, file_name):
    new_file_name = os.path.join(file_name.split('.')[0]) + '.fasta'
    new_file_path = os.path.join(file_dest_path, new_file_name)
    with gzip.open(file_path, "rb") as tsv_file:
        data = pandas.read_csv(tsv_file ,sep='\t')
        sequence_id = data['sequence_id']
        sequence = data['sequence']
        

        with open(new_file_path, 'w') as fasta_file:
            for i in range(0, len(sequence_id)):
                to_write = '>' + sequence_id[i] + '\n' + sequence[i] + '\n'
                fasta_file.write(to_write)



def start_extraction(project_name):
    # Create the parser
    try:
        reperoires_paths = find_all_repertoires(project_name)
        create_preprocessed_structure(project_name, reperoires_paths)
    except Exception as e:
        print(e)

