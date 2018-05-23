#!/usr/bin/env bash
##################################################################
## A welcome script run after Sparkall docker image is built    ##
## in order to provide users with some instruction  .           ##
##################################################################
# Defining some colors in terminal.
RED='\033[0;31m'
GREEN='\033[0;32m'
BLACK='\033[0;33m'
BLUE='\033[0;34m'	
NC='\033[0m'
echo -e "/--------------------------------------------------------------------------------\\"
echo -e "|                         ${Sparkall}=== Sparkall Demonstrator ===${NC}"
echo -e "|"
echo -e "| ${GREEN}What is this?${NC}"
echo -e "|   This is a docker image that allows to reproduce Sparkall results. It: \n"
echo -e "|       1. Installs 3 databases: Cassandra, MongoDB and MySQL. \n"
echo -e "|       2. Downloads necessary input files: config, mappings.ttl and 9 queries. \n"
echo -e "|       3. Generates 5 BSBM SQL dumps about: Product, Review, Offer, Person and Producer. \n"
echo -e "|       4. Loads the dumps into: Cassandra, MongoDB, Parquet, CSV and MySQL, respectively. \n"
echo -e "|       5. Runs the 9 (SPARQL) queries over the data and saves results to a file. \n"
echo -e "|"
echo -e "| ${GREEN}What to do?${NC}"
echo -e "|   You run the following in this order:"
echo -e "|      1. Load data using: ${RED}\`bash ~/load-data.sh\`${NC}."
echo -e "|      2. Run queries over the loaded data (${BLACK}you can change [*] (replace * with the nbr of cores to affect), 8GB (memory to affect) and results.txt (output file) values${NC}): \n"
echo -e "|      ${RED}\`bash ~/run-sparkall.sh local[*] 8G /usr/local/sparkall/evaluation/queries /usr/local/sparkall/evaluation/mappings.ttl /usr/local/sparkall/evaluation/config r results.txt\`${NC}"
echo -e "|   - Yo can also run Sparkall over a single query, specify its file inside 'queries' directory, e.g., '.../queries/Q7.sparql'"
echo -e "|"
echo -e "| ${GREEN}Credits:${NC}"
echo -e "|   Mohamed Nadjib Mami, EIS @ Fraunhofer IAIS"
echo -e "|   ${BLUE}mohamed.nadjib.mami@iais.Fraunhofer.de"${NC}
echo -e "|   2018"
echo -e "\\--------------------------------------------------------------------------------/"
