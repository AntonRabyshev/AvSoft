#!/bin/bash

# Start the first process
python ./sender.py &
# Start the second process
python ./parser.py &
# Start the third process
python ./error_handler.py &
# Start the fourth process
# следующий процесс выполняется in-place, поэтому не запускаем
#python ./file_generator.py &
