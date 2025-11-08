"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import glob
import os
import string
import time

def wordcount_mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("","", string.punctuation))
        line = line.replace("\n","")
        words = line.split()
        pairs_sequence.extend([(word,1) for word in words])
    return pairs_sequence

def wordcount_reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key,result[-1][1] + value)
        else:
            result.append((key,value))
    return result

def mapreduce(mapper, reducer, input_dir, output_dir):
    def collect_lines_from_files():
        sequence = []
        files = glob.glob(os.path.join(input_dir, "*"))
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence
    
    def apply_shuffle_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence
    
    def write_word_count_to_file(result):
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "part-00000")
        with open(out_path, "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")
                
    def create_success_file():
        os.makedirs(output_dir, exist_ok=True)
        success_path = os.path.join(output_dir, "_SUCCESS")
        with open(success_path, "w", encoding="utf-8") as f:
            f.write("")
    
    def prepare_output_dir():
        if os.path.exists(output_dir):
            raise FileExistsError(f"Output directory '{output_dir}' already exists. Aborting to avoid data loss.")
        os.makedirs(output_dir, exist_ok=False)

    prepare_output_dir()
    
    sequence = collect_lines_from_files()
    pairs_sequence = mapper(sequence)
    pairs_sequence = apply_shuffle_and_sort(pairs_sequence)
    result = reducer(pairs_sequence)
    write_word_count_to_file(result)
    create_success_file()
    
def run_experiment(n, mapper, reducer, raw_dir, input_dir, output_dir):
    def initialize_directory(directory, remove_dir=False):
        if os.path.exists(directory):
            for path in glob.glob(os.path.join(directory, "*")):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    # remove nested directories and their contents
                    for root, dirs, files in os.walk(path, topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in dirs:
                            os.rmdir(os.path.join(root, name))
                    os.rmdir(path)
            if remove_dir:
                try:
                    os.rmdir(directory)
                except OSError:
                    # directory may already be removed or not empty; ignore here
                    pass
        else:
            os.makedirs(directory, exist_ok=True)
            
    def create_numbered_files_copies(raw_dir, input_dir, n):
        os.makedirs(input_dir, exist_ok=True)
        for file in glob.glob(os.path.join(raw_dir, "*")):
            with open(file, "r", encoding="utf-8") as f:
                text = f.read()
            base = os.path.splitext(os.path.basename(file))[0]
            for i in range(1, n + 1):
                new_filename = f"{base}_{i}.txt"
                with open(os.path.join(input_dir, new_filename), "w", encoding="utf-8") as f2:
                    f2.write(text)
    
    # Ensure input dir is empty and output dir does not exist
    initialize_directory(input_dir, remove_dir=False)
    initialize_directory(output_dir, remove_dir=True)
    
    create_numbered_files_copies(raw_dir, input_dir, n)
            
    start_time = time.time()
    mapreduce(mapper, reducer, input_dir, output_dir)
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")

if __name__ == "__main__":

    run_experiment(1000, 
                   wordcount_mapper, 
                   wordcount_reducer, 
                   "files/raw", 
                   "files/input", 
                   "files/output")