import csv
import os

class SimpleDB:
    def __init__(self):
        self.relations = {}

    def load_relations(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                relation_name = filename[:-4]  # remove .csv
                with open(os.path.join(directory, filename), 'r') as file:
                    reader = csv.reader(file)
                    attributes = next(reader)  # first row is attributes
                    data = [row for row in reader]  # remaining rows are data
                    self.relations[relation_name] = {'attributes': attributes, 'data': data}

    def process_queries_from_file(self, query_file, output_file):
        with open(query_file, 'r') as f:
            queries = f.readlines()

        results = []
        for query in queries:
            query = query.strip()
            try:
                result = self.execute_query(query)
                results.append((query, result))
            except Exception as e:
                results.append((query, f"Error: {str(e)}"))

        # Write results to output file
        with open(output_file, 'w', newline='') as out_file:
            writer = csv.writer(out_file)
            for query, result in results:
                writer.writerow([query])
                if isinstance(result, list):
                    writer.writerows(result)
                writer.writerow([])  # blank line between queries

    def execute_query(self, query):
        # Handle nested queries for union and difference
        if query.startswith('(') and query.endswith(')'):
            query = query[1:-1].strip()

        # Check for UNION
        if 'U' in query:
            left_query, right_query = self.parse_union_query(query)
            return self.union(left_query, right_query)

        # Check for DIFFERENCE
        elif '-' in query:
            left_query, right_query = self.parse_difference_query(query)
            return self.difference(left_query, right_query)

        # Handle other queries
        elif query.startswith("SELE"):
            return self.selection(query)
        elif query.startswith("PROJ"):
            return self.projection(query)
        elif query.startswith("X"):
            return self.crossproduct(query)
        return []

    def parse_union_query(self, query):
        left, right = query.split('U', 1)
        return left.strip(), right.strip()

    def parse_difference_query(self, query):
        left, right = query.split('-', 1)
        return left.strip(), right.strip()

    def selection(self, query):
        relation_name = query.split('(')[-1].split(')')[0].strip()
        condition = query.split('{')[1].split('}')[0].strip()

        if relation_name not in self.relations:
            raise ValueError(f"Relation '{relation_name}' not found.")

        data = self.relations[relation_name]["data"]
        attr, operator, value = condition.split()
        value = value.strip("'")

        if attr not in self.relations[relation_name]["attributes"]:
            raise ValueError(f"Attribute '{attr}' not found in relation '{relation_name}'.")

        col_index = self.relations[relation_name]["attributes"].index(attr)
        filtered_data = [row for row in data if self.evaluate_condition(row[col_index], operator, value)]
        return filtered_data

    def projection(self, query):
        relation_name = query.split('(')[-1].split(')')[0].strip()
        attributes = query.split('{')[1].split('}')[0].strip().split(',')
        attributes = [attr.strip() for attr in attributes]

        if relation_name not in self.relations:
            raise ValueError(f"Relation '{relation_name}' not found.")

        data = self.relations[relation_name]["data"]
        rel_attributes = self.relations[relation_name]["attributes"]

        col_indices = []
        for attr in attributes:
            if attr not in rel_attributes:
                raise ValueError(f"Attribute '{attr}' not found in relation '{relation_name}'.")
            col_indices.append(rel_attributes.index(attr))

        projected_data = [[row[i] for i in col_indices] for row in data]
        return projected_data

    def difference(self, query):
        # Split the query into left and right parts
        left_query, right_query = query.split('-')

        # Process the left and right queries to get results
        left_result = self.process_single_query(left_query.strip())
        right_result = self.process_single_query(right_query.strip())

        if left_result is None or right_result is None:
            return None

        # Get the header from the left result
        left_header = left_result[0]
        right_header = right_result[0]

        # Check if the projections are consistent
        if left_header != right_header:
            raise ValueError("Headers do not match for the difference operation.")

        # Perform the difference operation
        left_set = {tuple(row) for row in left_result[1:]}  # Exclude header
        right_set = {tuple(row) for row in right_result[1:]}  # Exclude header

        # Calculate difference
        difference_set = left_set - right_set

        # Convert to list of lists for return
        return [left_header] + [list(row) for row in difference_set]

    def process_single_query(self, query):
        query = query.strip()
        if query.startswith("SELE"):
            return self.selection(query)
        elif query.startswith("PROJ"):
            return self.projection(query)
        elif query.startswith("X"):
            return self.crossproduct(query)
        elif query.startswith("JOIN"):
            return self.join(query)
        elif query.startswith("*"):
            return self.naturaljoin(query)
        elif query.startswith("U"):
            return self.union(query)
        elif query.startswith("-"):
            return self.difference(query)
        # Add any additional operations as needed
        return None

    def union(self, left_query, right_query):
        left_result = self.execute_query(left_query)
        right_result = self.execute_query(right_query)

        # Merge results and remove duplicates
        union_result = left_result + [row for row in right_result if row not in left_result]
        return union_result

    def evaluate_condition(self, value, operator, condition_value):
        if operator == '>':
            return float(value) > float(condition_value)
        elif operator == '<':
            return float(value) < float(condition_value)
        elif operator == '=':
            return value == condition_value
        return False

# Example usage
db = SimpleDB()
db.load_relations(r'Data')  # Directory containing your CSV files
db.process_queries_from_file(r'C:\Project 1\RAqueries.txt', r'C:\Project 1\RAoutput.csv')
