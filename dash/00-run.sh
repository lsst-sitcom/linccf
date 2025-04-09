#!/bin/bash

run_dash() {
    parse_input_arguments $@
    print_banner
    setup_lsst_stack
    create_output_dir
    create_log_file
    fetch_stages
    run_stages
}

parse_input_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --DRP_VERSION)
                DRP_VERSION=$2
                shift 2
                ;;
            --COLLECTION_TAG)
                COLLECTION_TAG=$2
                shift 2
                ;;
            *)
                echo "Usage: $0 --DRP_VERSION version --COLLECTION_TAG tag"
                exit 1
                ;;
        esac
    done
    if [[ -z $DRP_VERSION || -z $COLLECTION_TAG ]]; then
        echo "Error: Both --DRP_VERSION and --COLLECTION_TAG arguments are required."
        echo "Usage: $0 --DRP_VERSION version --COLLECTION_TAG tag"
        exit 1
    fi
    export DRP_VERSION
    export COLLECTION_TAG
}

print_banner() {
    echo "----- DASH Import Pipeline -----"
    echo "Starting import of $DRP_VERSION..."
}

setup_lsst_stack() {
    printf "Updating to the latest LSST Stack...\n"
    source /sdf/group/rubin/sw/loadLSST.sh
    setup lsst_distrib
}

create_output_dir() {
    output_dir="outputs/$DRP_VERSION"
    if [ -d $output_dir ]; then
        echo "Directory $output_dir already exists! Exiting..." >&2
        exit 1
    fi
    mkdir -p $output_dir
}

create_log_file() {
    output_file="$output_dir/runtimes.tsv"
    touch $output_file
    formatting_header="%-40s\t%-20s\n" # Header row for fixed column alignment
    printf $formatting_header "Stage name" "Runtime (HH:MM:SS)" > $output_file
}

fetch_stages() {
    mapfile -t stages < <(find . -maxdepth 1 -type f -name "*.ipynb" ! -name ".*" | sort)
    echo "Stages to be executed (in order):"
    for stage in "${stages[@]}"; do
        echo $stage
    done
}

run_stages() {
    total_runtime=0

    # Execute each stage notebook, and log execution time
    for stage in "${stages[@]}"; do
        printf "Executing $stage...\n"
        output_notebook="$output_dir/$(basename $stage)"
        
        start_time=$(date +%s)

        # Execute the notebook using jupyter nbconvert
        jupyter nbconvert \
            --to notebook \
            --execute $stage \
            --output $output_notebook \
            --log-level=CRITICAL
        
        end_time=$(date +%s)
        runtime=$((end_time - start_time))
        total_runtime=$((total_runtime + runtime))
        print_runtime_readable_format $stage runtime
        
        # Log results into our TSV file
        printf $formatting_header $stage $runtime_str >> $output_file
    done

    print_runtime_readable_format "Total_runtime" total_runtime
    printf $formatting_header "Total_runtime" $runtime_str >> $output_file
    echo "DASH pipeline finished. Logs in $output_dir."
}

print_runtime_readable_format() {
    local stage_name=$1
    local runtime_seconds=$2

    local hours=$((runtime_seconds / 3600))
    local minutes=$(((runtime_seconds % 3600) / 60))
    local seconds=$((runtime_seconds % 60))

    runtime_str=$(printf "%02d:%02d:%02d" $hours $minutes $seconds)
    
    # Print in HH:MM:SS format with zero padding
    printf $formatting_header $stage_name "$runtime_str (HH:MM:SS)"
}

run_dash $@