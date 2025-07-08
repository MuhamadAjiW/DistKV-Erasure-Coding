#!/bin/bash
sudo chown -R "$(id -un):$(id -gn)" .

# Organize data files into directories based on payload, load, and network bandwidth
organize_files() {
    BASE_DIR="$(dirname "$0")/benchmark/results/trace"

    echo "Organizing files in $BASE_DIR..."

    # Loop through all relevant files in data subdirectories
    find "$BASE_DIR" -type f \( -name "*.bench" -o -name "*.txt" -o -name "*.json" -o -name "*.log" \) | while read -r file; do
        echo "Processing file: $file"

        fname="$(basename "$file")"
        # Extract payload size (number before 'b')
        if [[ "$fname" =~ ([0-9]+)b ]]; then
            payload=${BASH_REMATCH[1]}
        else
            continue
        fi
        # Determine load tag
        if [[ "$payload" -eq 1024 ]]; then
            load_tag="smload"
        else
            load_tag=""
        fi
        # Extract bandwidth token
        if [[ "$fname" =~ ([0-9]+(gbit|kbit)) ]]; then
            bw_token=${BASH_REMATCH[1]}
        else
            bw_token=""
        fi
        # Map bandwidth to network category
        case "$bw_token" in
            10gbit)
                net_tag="fastnet" ;;
            256kbit)
                net_tag="slownet" ;;
            *)
                net_tag="avgnet" ;;
        esac
        # Determine operation (read or write) by path first, then filename
        if [[ "$file" =~ /read/ ]]; then
            op_tag="read"
        elif [[ "$file" =~ /write/ ]]; then
            op_tag="write"
        elif [[ "$fname" =~ read ]]; then
            op_tag="read"
        elif [[ "$fname" =~ put|write ]]; then
            op_tag="write"
        else
            continue
        fi
        # Build target directory name
        if [[ -n "$load_tag" ]]; then
            target_dir="${op_tag}_${load_tag}_${net_tag}"
        else
            target_dir="${op_tag}_${net_tag}"
        fi
        # Create and move
        dest_path="$BASE_DIR/organized/$target_dir"
        mkdir -p "$dest_path"
        mv -n "$file" "$dest_path/"
    done
}

# Run organization
organize_files
