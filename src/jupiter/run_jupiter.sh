#!/bin/bash

# Parse the YAML file using yq

FILE_DIR="$(dirname "$(realpath "$0")")"
#!/bin/bash


commands="$FILE_DIR/jupiter-swap-api "
# Read the YAML file line by line


while read -r line; do
  # Check if the line starts with a # using grep. If it does, skip to the next iteration.
    if echo "$line" | grep -q '^#'; then
    continue
  fi


  # Use yq to parse the line and store the result in a variable

  variable=$(echo "$line" | yq 'to_entries |  from_entries')


  #Use IFS as yq o=shell coerces the - to be _
  IFS=': ' read -r key value <<< "$variable"

  commands+="--$key=$value "
  # Use the variable in your script
done < jupiterConfig.yaml

echo $commands
$commands
