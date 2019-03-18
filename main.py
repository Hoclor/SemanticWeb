from SPARQLWrapper import SPARQLWrapper, JSON
import time

repetitions = 100 # Number of experiments to carry out
limits_list = [250, 500, 1000, 2000, 4000] # LIMIT values to investigate
joins_list = [1,2,3,4,5] # Which numbers of joins to investigate - between 1 and 5

# Define functions to create the queries
def prepare_inner_join_query(limit, joins):
    extra_queries = [
        " .\n?person foaf:gender ?sex",
        " .\n?person dbo:partner ?partner",
        " .\n?partner foaf:name ?partnername",
        " .\n?partner foaf:gender ?partnersex"
    ]
    string_parts = []
    # Base query
    string_parts.append("""
    PREFIX dbo:<http://dbpedia.org/ontology/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT DISTINCT *\n""")

    # Add the queries 
    string_parts.append("""WHERE {
        ?person a dbo:Person .
        ?person foaf:name ?name""")
    for i in range(0, joins-1):
        string_parts.append(extra_queries[i])
    string_parts.append("\n}")
    
    # Add the limit
    string_parts.append("\nLIMIT {}".format(limit))

    # Construct the query
    query = ''.join(string_parts)

    return query

def prepare_sequential_left_join_query(limit, joins):
    extra_queries = [
        "\n        OPTIONAL { ?person foaf:gender ?sex }",
        "\n        OPTIONAL { ?person dbo:partner ?partner }",
        "\n        OPTIONAL { ?partner foaf:name ?partnername }",
        "\n        OPTIONAL { ?partner foaf:gender ?partnersex }"
    ]
    string_parts = []
    # Base query
    string_parts.append("""
    PREFIX dbo:<http://dbpedia.org/ontology/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT DISTINCT *\n""")

    # Add the queries 
    string_parts.append("""WHERE {
        ?person a dbo:Person
        OPTIONAL { ?person foaf:name ?name }""")
    for i in range(0, joins-1):
        string_parts.append(extra_queries[i])
    string_parts.append("\n}")
    
    # Add the limit
    string_parts.append("\nLIMIT {}".format(limit))

    # Construct the query
    query = ''.join(string_parts)

    return query

def prepare_nested_left_join_query(limit, joins):
    extra_queries = [
        "\n        OPTIONAL { ?person foaf:gender ?sex",
        "\n        OPTIONAL { ?person dbo:partner ?partner",
        "\n        OPTIONAL { ?partner foaf:name ?partnername",
        "\n        OPTIONAL { ?partner foaf:gender ?partnersex"
    ]
    string_parts = []
    # Base query
    string_parts.append("""
    PREFIX dbo:<http://dbpedia.org/ontology/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT DISTINCT *\n""")

    # Add the queries 
    string_parts.append("""WHERE {
        ?person a dbo:Person
        OPTIONAL { ?person foaf:name ?name""")
    for i in range(0, joins-1):
        string_parts.append(extra_queries[i])
    string_parts.append("\n}")

    # Close the queries
    string_parts.append('\n        ')
    for i in range(joins):
        string_parts.append('}')
    
    # Add the limit
    string_parts.append("\nLIMIT {}".format(limit))

    # Construct the query
    query = ''.join(string_parts)

    return query

# First join: inner join (.)

# Full query:
# PREFIX dbo:<http://dbpedia.org/ontology/>
# PREFIX foaf: <http://xmlns.com/foaf/0.1/>
# SELECT *
# WHERE {
#   ?person a dbo:Person .
#   ?person foaf:name ?name .
#   ?person foaf:gender ?sex .
#   ?person dbo:partner ?partner .
#   ?partner foaf:name ?partnername .
#   ?partner foaf:gender ?partnersex
# }
# LIMIT 2000

# Each query carried out 100 times and wall clock time was averaged to try to increase accuracy of results
# Results still very varied due to nature of networks, connectivity, etc.

# First experimentation: LIMIT value (2000, 4000, 6000, 8000, 10000)

# Second experimentation: number of joins performed (1, 2, 3, 4, 5)


print('*****Inner join*****')

print("{:10}, {:5}, {:10}, {:5}".format("Limit", "Joins", "Num Results", "Avg time"))

# Create connection to dbpedia
sparql = SPARQLWrapper("http://dbpedia.org/sparql")
# Get the output in JSON format for easy parsing
sparql.setReturnFormat(JSON)
for limit in limits_list:
    for joins in joins_list:
        query = prepare_inner_join_query(limit, joins)
        sparql.setQuery(query)
        # Do one query to get the number of matches returned
        num_results = len(sparql.query().convert()["results"]["bindings"]) 
        # Perform each query `repetitions` times to produce an average time required
        times_taken = []
        for i in range(repetitions):
            sparql.setQuery(query)
            start = time.time()
            results = sparql.query() # Do the SPARQL query
            times_taken.append(time.time() - start)
        # Sort the times_taken list
        times_taken = sorted(times_taken)
        # Discard the bottom and upper quartiles of time values, as higher/lower values are likely to be outliers due to network connectivity issues. This will return a much better estimate for avg time taken
        middle_50_times = times_taken[(repetitions//4):(repetitions//4)*3]
        avg = sum(middle_50_times)/len(middle_50_times)
        # Print the results
        print("{:10}, {:5}, {:10}, {:5}".format(limit, joins, num_results, avg))

print("\ndone\n")


# Second join: left join (optional)

# Full query:
# PREFIX dbo:<http://dbpedia.org/ontology/>
# PREFIX foaf: <http://xmlns.com/foaf/0.1/>
# SELECT *
# WHERE {
#   ?person a dbo:Person
#   OPTIONAL { ?person foaf:name ?name }
#   OPTIONAL { ?person foaf:gender ?sex }
#   OPTIONAL { ?person dbo:partner ?partner }
#   OPTIONAL { ?partner foaf:name ?partnername }
#   OPTIONAL { ?partner foaf:gender ?partnersex }
# }
# LIMIT 2000

# First experimenation: LIMIT value (same range as above)

# Second experimentation: number of joins performed (same range as above)

# Third experimentation: nested or sequential consecutive joins

print("*****Sequential left join*****")

print("{:10}, {:5}, {:10}, {:5}".format("Limit", "Joins", "Num Results", "Avg time"))

# Create connection to dbpedia
sparql = SPARQLWrapper("http://dbpedia.org/sparql")
# Get the output in JSON format for easy parsing
sparql.setReturnFormat(JSON)
for limit in limits_list:
    for joins in joins_list:

        sparql.setQuery(prepare_sequential_left_join_query(limit, joins))
        # Do one query to get the number of matches returned
        num_results = len(sparql.query().convert()["results"]["bindings"]) 
        # Perform each query `repetitions` times to produce an average time required
        times_taken = []
        for i in range(repetitions):
            start = time.time()
            results = sparql.query() # Do the SPARQL query
            times_taken.append(time.time() - start)
        # Sort the times_taken list
        times_taken = sorted(times_taken)
        # Discard the bottom and upper quartiles of time values, as higher/lower values are likely to be outliers due to network connectivity issues. This will return a much better estimate for avg time taken
        middle_50_times = times_taken[(repetitions//4):(repetitions//4)*3]
        avg = sum(middle_50_times)/len(middle_50_times)
        # Print the results
        print("{:10}, {:5}, {:10}, {:5}".format(limit, joins, num_results, avg))

print("\ndone\n")

print("*****Nested left join*****")

print("{:10}, {:5}, {:10}, {:5}".format("Limit", "Joins", "Num Results", "Avg time"))

# Create connection to dbpedia
sparql = SPARQLWrapper("http://dbpedia.org/sparql")
# Get the output in JSON format for easy parsing
sparql.setReturnFormat(JSON)
for limit in limits_list:
    for joins in joins_list:

        sparql.setQuery(prepare_nested_left_join_query(limit, joins))
        # Do one query to get the number of matches returned
        num_results = len(sparql.query().convert()["results"]["bindings"]) 
        # Perform each query `repetitions` times to produce an average time required
        times_taken = []
        for i in range(repetitions):
            start = time.time()
            results = sparql.query() # Do the SPARQL query
            times_taken.append(time.time() - start)
        # Sort the times_taken list
        times_taken = sorted(times_taken)
        # Discard the bottom and upper quartiles of time values, as higher/lower values are likely to be outliers due to network connectivity issues. This will return a much better estimate for avg time taken
        middle_50_times = times_taken[(repetitions//4):(repetitions//4)*3]
        avg = sum(middle_50_times)/len(middle_50_times)
        # Print the results
        print("{:10}, {:5}, {:10}, {:5}".format(limit, joins, num_results, avg))

print("\ndone\n")
