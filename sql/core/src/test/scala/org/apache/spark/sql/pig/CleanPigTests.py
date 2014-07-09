"""
 " CleanPigTests.py
 " Strips whitespace in Pig tests; replaces Lisp-style :: escape sequences with
 "  Java-style $ escape sequences
"""

from os import listdir
from os.path import isfile, join
import re

TEST_DIR = "/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/pig/tests"

filenames = [f for f in listdir(TEST_DIR) if isfile(join(TEST_DIR, f))]

def clean(line):
    stripped = line.strip()
    escaped = re.sub(r':(\w+):', r'$\1', stripped)
    output = escaped + "\n"
    return output

for name in filenames:
    path = join(TEST_DIR, name)
    new_lines = []
    with open(path, 'r') as rf:
        new_lines = [clean(line) for line in rf.readlines()]
    with open(path, 'w') as wf:
        for line in new_lines:
            wf.write(line)
