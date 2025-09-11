"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby

from toolz.itertoolz import concat, pluck



def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""



def load_input(input_directory):
    """Funcion load_input"""


def preprocess_line(x):
    """Preprocess the line x"""


def map_line(x):
    pass

def mapper(sequence):
    """Mapper"""


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""



def compute_sum_by_group(group):
    pass

def reducer(sequence):
    """Reducer"""


def create_directory(directory):
    """Create Output Directory"""


def save_output(output_directory, sequence):
    """Save Output"""


def create_marker(output_directory):
    """Create Marker"""


def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
