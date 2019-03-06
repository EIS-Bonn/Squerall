#!/usr/bin/env bash
##################################################################
## A welcome script run after Squerall docker image is built    ##
## in order to provide users with some instruction  .           ##
##################################################################
# Defining some colors in terminal.
RED='\033[0;31m'
GREEN='\033[0;32m'
BLACK='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'
echo -e "/--------------------------------------------------------------------------------\\"
echo -e "|                         ${RED}=== Squerall Demonstrator ===${NC}"
echo -e "|"
echo -e "| ${GREEN}What is this?${NC}"
echo -e "|   This is a docker image that allows to reproduce Squerall results. It:"
echo -e "|       1. Installs 3 databases: Cassandra, MongoDB and MySQL."
echo -e "|       2. Downloads necessary input files: config, mappings.ttl and 9 queries."
echo -e "|       3. Generates 5 BSBM SQL dumps about: Product, Review, Offer, Person and Producer."
echo -e "|       4. Loads the dumps into: Cassandra, MongoDB, Parquet, CSV and MySQL, respectively."
echo -e "|       5. Runs the 9 (SPARQL) queries over the data and saves results to a file."
echo -e "|"
echo -e "| ${GREEN}What to do?${NC}"
echo -e "|   You run the following in this order:"
echo -e "|      1. Load data using: ${RED}\`bash ~/load-data.sh\`${NC}."
echo -e "|      2. Run queries over the loaded data using the following command, for Spark: \n"
echo -e "|      ${RED}\`bash ~/run-squerall.sh local[*] 8G /root/input/queries /root/input/mappings.ttl /root/input/config n s results.txt\`${NC}"
echo -e "|      For Presto, replace local[*] with Presto server URL:"
echo -e "|      ${RED}\`bash ~/run-squerall.sh jdbc:presto://localhost:8080 8G /root/input/queries /root/input/mappings.ttl /root/input/config n p results.txt\`${NC}"
echo -e "|      (${BLACK}you can change * (nbr of cores to affect to Spark cluster), 8GB (memory to affect) and results.txt (output file)${NC})"
echo -e "|   - You can also run Squerall over a single query, specify its file inside 'queries' directory, e.g., '.../queries/Q7.sparql'"
echo -e "|   - Open results.txt file to view the detailed execution steps, actual results, as well as number of results and execution times."
echo -e "|"
echo -e "| ${GREEN}Contact:${NC}"
echo -e "|   Mohamed Nadjib Mami, EIS @ Fraunhofer IAIS"
echo -e "|   ${BLUE}mohamed.nadjib.mami@iais.fraunhofer.de"${NC}
echo -e "|   2018"
echo -e "\\--------------------------------------------------------------------------------/"
