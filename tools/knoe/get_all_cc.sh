#!/bin/bash

# 指定要搜索的目录
declare -a directories=("cache" "db" "env" "file" "logging" 
                        "memory" "memtable" "monitoring" "options" "port" 
                        "table" "test_util" "tools" 
                        "trace_replay" "util" "utilities")

# 搜索目录下所有的cc文件，并将文件名加上相对路径信息保存到结果文件中
for dir in "${directories[@]}"; do
    find "$dir" -type f -name '*.cc' ! -name "*test.cc" | while read file; do
        relative_path=$(realpath --relative-to="$PWD" "$file")
        echo "$relative_path" | sed -e 's/.*/"&",/' >> all_cc_file_name
    done
done