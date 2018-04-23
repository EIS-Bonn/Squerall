#!/bin/bash
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
echo -e "|   This is a docker image that allows to reprodue Sparkall results. It: \n"
echo -e "|       1. Installs 3 databases: Cassandra, MongoDB and MySQL. \n"
echo -e "|       2. Downloadd sample Sparkall input files: config, mappings.ttl and 9 queries. \n"
echo -e "|       3. Generates 5 BSBM SQL dumps about: Product, Review, Offer, Person and Producer. \n"
echo -e "|       4. Loads the dumps into: Cassandra, MongoDB, Parquet, CSV and MySQL, respectively. \n"
echo -e "|       5. Runs the 10 (SPARQL) queries over the 5 databases and save results to a file. \n"
echo -e "|"
echo -e "| ${GREEN}What to do?${NC}"
echo -e "|   You can either:"
echo -e "|      1. Run the image as-is, so all is generated for you: ${RED}\`bash ~/run-sparkall.sh\`${NC}."
#echo -e "|     2. You provide BSBM data generation scale factor: ${RED}\`bash ~/run-sparkall.sh [scale_factore]\`${NC}."
echo -e "|"
echo -e "| ${GREEN}Credits:${NC}"
echo -e "|   Mohamed Nadjib Mami, EIS @ Fraunhofer IAIS"
echo -e "|   2018"
echo -e "\\--------------------------------------------------------------------------------/"
