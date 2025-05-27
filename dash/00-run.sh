#!/bin/bash
set -e

run_dash() {
    parse_input_arguments $@
    print_banner
    setup_lsst_stack
    create_output_dir
    create_log_file
    fetch_stages
    run_stages

    if [ "$INSTRUMENT" = "LSSTCam" ]; then
        upload_to_embargo
    fi
}

parse_input_arguments() {
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == --* ]]; then
            key="${1#--}"  # Strip leading --
            val="$2"
            if [[ -z "$val" || "$val" == --* ]]; then
                echo "Error: Missing value for --$key"
                exit 1
            fi
            export "$key=$val"
            shift 2
        else
            echo "Unknown argument: $1"
            exit 1
        fi
    done

    missing=0
    for var in INSTRUMENT REPO RUN VERSION COLLECTION OUTPUT_DIR; do
        if [ -z "${!var}" ]; then
            echo "Error: --$var is required"
            missing=1
        fi
    done
    if [ "$missing" -ne 0 ]; then
        echo "Usage: $0 --INSTRUMENT name --REPO path --RUN id --VERSION version --COLLECTION tag --OUTPUT_DIR dir"
        exit 1
    fi
}

print_banner() {
    echo "----- DASH Import Pipeline -----"
    echo "Starting import of $VERSION..."
}

setup_lsst_stack() {
    printf "Updating to the latest LSST Stack...\n"
    source /sdf/group/rubin/sw/loadLSST.sh
    setup lsst_distrib
}

create_output_dir() {
    output_dir="outputs/$VERSION"
    if [ -d "$output_dir" ]; then
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

upload_to_embargo() {
    # See aws-cli#9214
    pip install awscli==1.36.0 --quiet
    
    local s3_bucket="s3://rubin-lincc-hats"

    local hats_dir=$OUTPUT_DIR/hats/$VERSION
    local raw_dir=$OUTPUT_DIR/raw/$VERSION
    local validation_dir=$OUTPUT_DIR/validation/$VERSION

    echo "Uploading $hats_dir..."
    aws s3 cp $hats_dir $s3_bucket/hats/$VERSION --recursive

    echo "Uploading $raw_dir..."
    aws s3 cp $raw_dir $s3_bucket/raw/$VERSION --recursive
    
    echo "Uploading $validation_dir..."
    aws s3 cp $validation_dir $s3_bucket/validation/$VERSION --recursive
 
    echo "Removing all data from temporary local storage..."
    rm -rf $hats_dir $raw_dir $validation_dir
}

run_dash $@