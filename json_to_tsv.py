import argparse
import airr
import json
import csv
import os

from airr import load_rearrangement, dump_rearrangement, validate_rearrangement, validate_airr, read_airr

SOURCE = '/misc/work'

METADATA_PATH = os.path.join(SOURCE, 'sequence_data_store/PROJECT_NAME/project_metadata/metadata.json')
BIOSAMPLE_OUTPUT = os.path.join(SOURCE, 'sequence_data_store/PROJECT_NAME/project_metadata/biosample.tsv')
SRA_OUTPUT = os.path.join(SOURCE, 'sequence_data_store/PROJECT_NAME/project_metadata/sra.tsv')

BIOSAMPLE_JSON_FORMAT_PATH = os.path.join(SOURCE, 'genotype_tool-command_line/json_formats/AIRR_BioSample_v1.0.json')
SRA_JSON_FORMAT_PATH = os.path.join(SOURCE, 'genotype_tool-command_line/json_formats/AIRR_SRA_v1.0.json')


# METADATA_PATH = r'C:\Users\yaniv\Desktop\work\yaarilab-genotype_tool-command_line\sequence_data_store\PROJECT_NAME\project_metadata\metadata.json'
# BIOSAMPLE_OUTPUT = r'C:\Users\yaniv\Desktop\work\yaarilab-genotype_tool-command_line\sequence_data_store\PROJECT_NAME\project_metadata\biosample.tsv'
# SRA_OUTPUT = r'C:\Users\yaniv\Desktop\work\yaarilab-genotype_tool-command_line\sequence_data_store\PROJECT_NAME\project_metadata\sra.tsv'

# BIOSAMPLE_JSON_FORMAT_PATH = r'C:\Users\yaniv\Desktop\work\yaarilab-genotype_tool-command_line\json_formats\AIRR_BioSample_v1.0.json'
# SRA_JSON_FORMAT_PATH = r'/misc/work/genotype_tool-command_line/json_formats/AIRR_SRA_v1.0.json'

# Function to create BIOSAMPLE data file
def airr_biosamaple():
    
    try:
        data = read_airr(METADATA_PATH)
        tsv_file = open(BIOSAMPLE_OUTPUT, 'w', newline='')
        with open(BIOSAMPLE_JSON_FORMAT_PATH, 'r') as json_format:
            format = json.load(json_format)
            create_columns(tsv_file, format)
            
            for repertoire in data['Repertoire']:
                write_biosample_repertoire_line(tsv_file, repertoire,format)

    except Exception as e:
        print(e)

# Function to write a single line of BIOSAMPLE data
def write_biosample_repertoire_line(tsv_file, repertoire, format):
    tsv_row = []
    for key, value in format.items():
        parent = value
        if '.' in value:
            parent = value.split('.')[0]
            child = value.split('.')[1]
            new_value = check_biosample_parent_and_child(parent, child, value, repertoire)
            tsv_row.append(new_value)

        else:
            new_value = ''
            if parent in repertoire:
                new_value = repertoire[parent]
            
            tsv_row.append(new_value)

    write_row(tsv_file,tsv_row)

# def check_biosample_parent_and_child(parent,child,value, repertoire):
#     if child == 'diagnosis':
#         grandson = value.split('.')[2]
#         new_value = repertoire[parent][child][0][grandson]
    
#     elif parent == 'sample':
#         new_value = repertoire[parent][0][child]

#     else:
#         new_value = repertoire[parent][child]

#     return new_value


# Function to check and retrieve BIOSAMPLE data based on parent-child relationship
def check_biosample_parent_and_child(parent, child, value, repertoire):
    try:
        if parent in repertoire:
            parent_obj = repertoire[parent]
            
            # Check if the parent object is a list and has at least one element
            if isinstance(parent_obj, list) and len(parent_obj) > 0:
                first_parent = parent_obj[0]

                if len(value.split('.')) == 3:
                    grandson = value.split('.')[2]
                    return child_obj[0].get(grandson, '')

                # Check if the child exists in the first element of the parent list
                elif child in first_parent:
                    child_obj = first_parent.get(child, '')
                    return child_obj
            
            elif child in parent_obj:
                child_obj = parent_obj.get(child, '')
                if isinstance(child_obj, list) and len(child_obj) > 0:
                    if len(value.split('.')) == 3:
                        grandson = value.split('.')[2]
                        return child_obj[0].get(grandson, '')
                    
                return child_obj

        # Return a default value or handle the missing data case
    
    except Exception as e:
        print(f"problem with {value}", e)

    return ''

# Function to create SRA data file
def airr_sra():
    try:
        data = read_airr(METADATA_PATH)
        tsv_file = open(SRA_OUTPUT, 'w', newline='')
        with open(SRA_JSON_FORMAT_PATH, 'r') as json_format:
            format = json.load(json_format)
            create_columns(tsv_file, format)
            
            for repertoire in data['Repertoire']:
                write_sra_repertoire_line(tsv_file, repertoire,format)

    except Exception as e:
        print(e)

# Function to write a single line of SRA data
def write_sra_repertoire_line(tsv_file, repertoire, format):
    tsv_row = []
    for key, value in format.items():
        parent = value
        if value == None:
            tsv_row.append(None)
        
        elif value == "AIRR-seq":
            tsv_row.append(value)

        elif value == "":
            tsv_row.append("")

        elif '.' in value:
            parent = value.split('.')[0]
            child = value.split('.')[1]
            new_value = check_sra_parent_and_child(parent, child, value, repertoire)
            tsv_row.append(new_value)

    write_row(tsv_file,tsv_row)


# Function to check and retrieve SRA data based on parent-child relationship
def check_sra_parent_and_child(parent, child, value, repertoire):
    try:
        if parent in repertoire:
            parent_obj = repertoire[parent]

            # Check if the parent object is a list and has at least one element
            if isinstance(parent_obj, list) and len(parent_obj) > 0:
                first_parent = parent_obj[0]

                # Check for the child and possibly the grandson
                if child in first_parent:
                    child_obj = first_parent[child]
                    
                    
                    if isinstance(child_obj, list) and len(child_obj) > 0:
                        if len(value.split('.')) == 3:
                            grandson = value.split('.')[2]
                            return child_obj[0].get(grandson, '')
                        # else:
                        #     # Handle cases where there is no grandson
                        #     return child_obj[0]
                    else:
                        if len(value.split('.')) == 3:
                            grandson = value.split('.')[2]
                            return child_obj.get(grandson, '')
                        else:
                            return child_obj
            else:  
                if child in parent_obj:
                    return parent_obj[child]
        
    except Exception as e:
        print(f"problem with {value}", e)

    return ''

# Function to create columns in a TSV file based on a given format
def create_columns(tsv_file, format):

    tsv_columns = []
    for key, value in format.items():
        tsv_columns.append(key)

    writer = csv.writer(tsv_file, delimiter='\t')
    writer.writerow(tsv_columns)  # Write all column names in one row

# Function to write a row of data to a TSV file
def write_row(tsv_file,tsv_row):
    writer = csv.writer(tsv_file, delimiter='\t')
    writer.writerow(tsv_row) 

# Main function to create TSV files based on a project name
def create_tsv_files(project_name):
    global METADATA_PATH
    global BIOSAMPLE_OUTPUT
    global SRA_OUTPUT

    METADATA_PATH = METADATA_PATH.replace('PROJECT_NAME', project_name)
    BIOSAMPLE_OUTPUT = BIOSAMPLE_OUTPUT.replace('PROJECT_NAME', project_name)
    SRA_OUTPUT = SRA_OUTPUT.replace('PROJECT_NAME', project_name)

    airr_biosamaple()
    airr_sra()
    
