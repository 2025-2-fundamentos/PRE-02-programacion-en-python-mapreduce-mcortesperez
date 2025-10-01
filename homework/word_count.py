"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import glob
import os
import string
import time

# Crea la carpeta files/input
if os.path.exists("files/input/"):
    for file in glob.glob("files/input/*"):
        os.remove(file)
else:
    os.makedirs("files/input")


# Crea n copias de cada uno de los archivos en files/raw/
n = 5000

for file in glob.glob("files/raw/*"):

    with open(file, "r", encoding="utf-8") as f:
        text = f.read()

    for i in range(1, n + 1):
        raw_filename_with_extension = os.path.basename(file)
        raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[
            0
        ]
        new_filename = f"{raw_filename_without_extension}_{i}.txt"
        with open(f"files/input/{new_filename}", "w", encoding="utf-8") as f2:
            f2.write(text)


# El experimento realmente empieza en este punto.
start_time = time.time()


# Lee los archivos de files/input
sequence = []
files = glob.glob("files/input/*")
for file in files:
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            sequence.append((file, line))


# Mapea las líneas a pares (palabra, 1). Este es el mapper.
pairs_sequence = []
for _, line in sequence:
    line = line.lower()
    line = line.translate(str.maketrans("", "", string.punctuation))
    line = line.replace("\n", "")
    words = line.split()
    pairs_sequence.extend((word, 1) for word in words)

# Ordena la secuencia de pares por la palabra. Este es el shuffle and sort.
pairs_sequence = sorted(pairs_sequence)


# Reduce la secuencia de pares sumando los valores por cada palabra. Este es el reducer.
result = []
for key, value in pairs_sequence:
    if result and result[-1][0] == key:
        result[-1] = (key, result[-1][1] + value)
    else:
        result.append((key, value))

# Crea la carpeta files/output
if os.path.exists("files/output/"):
    for file in glob.glob(f"files/output/*"):
        os.remove(file)
else:
    os.makedirs("files/output")


# Guarda el resultado en un archivo files/output/part-00000
with open("files/output/part-00000", "w", encoding="utf-8") as f:
    for key, value in result:
        f.write(f"{key}\t{value}\n")


# Crea el archivo _SUCCESS en files/output
with open("files/output/_SUCCESS", "w", encoding="utf-8") as f:
    f.write("")

# El experimento finaliza aquí.
end_time = time.time()
print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")