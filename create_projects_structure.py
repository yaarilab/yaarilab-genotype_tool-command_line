import os
import shutil
import json
import unicodedata
import re
import time
from json_to_tsv import create_tsv_files

# Define the path where project data will be stored
PROJECTS_PATH = r"/misc/work/sequence_data_store/"
#PROJECTS_PATH = r"C:\Users\yaniv\Desktop\work\yaarilab-genotype_tool-command_line\yaarilab-genotype_tool-command_line\sequence_data_store"

def slugify(value, allow_unicode=False):
    # Converts a string into a slug format, which is easier to handle in file systems
    value = str(value)
    if allow_unicode:
        # Normalize unicode characters if allowed
        value = unicodedata.normalize("NFKC", value)
    else:
        # Convert to ASCII and ignore non-ASCII characters
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    # Remove any character that is not a word character, whitespace, or hyphen
    value = re.sub(r"[^\w\s-]", "", value.lower())
    # Replace spaces and repeated hyphens with a single hyphen
    return re.sub(r"[-\s]+", "-", value).strip("-_")

def create_new_structure(project):
    # Creates a new directory structure for a specific project
    project_path = os.path.join(PROJECTS_PATH, project)
    metadata_file = os.path.join(project_path, "metadata.json")
    
    with open(metadata_file, 'r') as metadata_file:
        metadata = json.load(metadata_file)
        # Create a folder for raw sequence data
        raw_seq_folder_path = os.path.join(project_path, "raw_seq")
        if not os.path.isdir(raw_seq_folder_path):
            os.mkdir(raw_seq_folder_path)

            # Organize data by subject and sample ID
            for repertoire in metadata["Repertoire"]:
                subject_id = slugify(repertoire["subject"]["subject_id"])
                subject_id_folder_path = os.path.join(raw_seq_folder_path, subject_id)
                if not os.path.isdir(subject_id_folder_path):
                    os.mkdir(subject_id_folder_path)
                
                for sample in repertoire["sample"]:
                    sample_id = slugify(sample["sample_id"])
                    sample_id_folder_path = os.path.join(subject_id_folder_path, sample_id)
                    if not os.path.isdir(sample_id_folder_path):
                        os.mkdir(sample_id_folder_path)
                
                # Move repertoire files to the corresponding sample folder
                repertoire_folder_path = os.path.join(sample_id_folder_path, repertoire["repertoire_id"])
                if not os.path.isdir(repertoire_folder_path):
                    os.mkdir(repertoire_folder_path)
                repertoire_path = os.path.join(project_path, repertoire["repertoire_id"] + ".tsv.gz")
                create_ids_json(repertoire["repertoire_id"], subject_id , sample_id, repertoire_folder_path)
                shutil.move(repertoire_path, repertoire_folder_path)


def create_ids_json(repertoire_id, subject_id, sample_id, repertoire_folder_path):
    json_path = os.path.join(repertoire_folder_path, 'repertoire_id.json')
    # Create a dictionary with the provided data
    data = {
        "repertoire_id": repertoire_id,
        "subject_id": subject_id,
        "sample_id": sample_id
    }

    # Write the dictionary to a JSON file at the specified path
    with open(json_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    


def move_metadata_file(project):
    # Moves the metadata file of a project to a specific folder
    project_path = os.path.join(PROJECTS_PATH, project)
    metadata_file_path = os.path.join(project_path, "metadata.json")
    remove_unicode_from_metadata(metadata_file_path)
    metadata_folder = os.path.join(project_path, "project_metadata")

    if not os.path.isdir(metadata_folder):
        os.mkdir(metadata_folder)
    
    shutil.move(metadata_file_path, metadata_folder)

def remove_unicode_from_metadata(file_path):
    # Removes non-ASCII characters from metadata files
    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read()
    cleaned_data = re.sub(r'[^\x00-\x7F]+', '', data)
    data_dict = json.loads(cleaned_data)
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data_dict, file, ensure_ascii=False, indent=4)

def start_new_structure(project_name):
    # Initiates the process of creating a new project structure
    print(f"creating new structure for {project_name}")
    create_new_structure(project_name)
    move_metadata_file(project_name)
    print(f"finished creating new structure for {project_name}")
    time.sleep(2)



    # Calls a function from another script to create TSV files
    #create_tsv_files(project_name)

