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
                if query.startswith("SELE"):
                    result = self.selection(query)
                elif query.startswith("PROJ"):
                    result = self.projection(query)
                elif query.startswith("X"):
                    result = self.crossproduct(query)
                elif query.startswith("JOIN"):
                    result = self.join(query)
                elif query.startswith("*"):
                    result = self.naturaljoin(query)
                elif query.startswith("U"):
                    result = self.union(query)
                elif query.startswith("-"):
                    result = self.difference(query)
                elif query.startswith(","):
                    result = self.AND(query)
                elif query.startswith("OR"):
                    result = self.OR(query)
                else:
                    result = None

                results.append((query, result))
                # Debug: Print each query result
                print(f"Query: {query}")
                print(f"Result: {result}")

            except Exception as e:
                results.append((query, f"Error: {str(e)}"))
                print(f"Error processing query '{query}': {str(e)}")

        # Write results to output file
        with open(output_file, 'w', newline='') as out_file:
            writer = csv.writer(out_file)
            for query, result in results:
                writer.writerow([query])
                if isinstance(result, list):
                    writer.writerows(result)
                writer.writerow([])  # blank line between queries

    def selection(self, query):
        # Extract relation and condition
        relation_name = query.split('(')[-1].split(')')[0].strip()
        condition = query.split('{')[1].split('}')[0].strip()

        # Find the relation
        if relation_name not in self.relations:
            raise ValueError(f"Relation '{relation_name}' not found.")

        data = self.relations[relation_name]["data"]
        conditions = self.parse_conditions(condition)

        # Evaluate condition
        filtered_data = [
            row for row in data if self.evaluate_conditions(row, conditions)
        ]
        return [self.relations[relation_name]["attributes"]] + filtered_data  # Include header

    def parse_conditions(self, condition_str):
        # Split conditions based on AND/OR
        conditions = []
        if 'AND' in condition_str:
            conditions.extend(cond.strip() for cond in condition_str.split('AND'))
        elif 'OR' in condition_str:
            conditions.extend(cond.strip() for cond in condition_str.split('OR'))
        else:
            conditions.append(condition_str)
        return conditions

    def evaluate_conditions(self, row, conditions):
        results = []
        for condition in conditions:
            if condition:  # Ignore empty conditions
                attr, operator, value = condition.split()
                value = value.strip("'")

                # Find the index of the attribute
                col_index = self.get_attribute_index(attr)

                # Evaluate the condition for this row
                results.append(self.evaluate_condition(row[col_index], operator, value))

        # Return True if all conditions are True for AND, or if any are True for OR
        return all(results)  # For AND conditions

    def projection(self, query):
        # Extract the relation and attributes to project
        relation_query = query.split('(')[-1].split(')')[0].strip()
        attributes = query.split('{')[1].split('}')[0].strip().split(',')
        attributes = [attr.strip() for attr in attributes]

        # If the relation involves a cross product (e.g., ACTORS * PAY), handle that first
        if '*' in relation_query:
            relation_data = self.crossproduct(f"{relation_query}")
            rel_attributes = relation_data[0]  # First row is the header (attributes)
            data = relation_data[1:]  # Remaining rows are the actual data
        else:
            # Otherwise, it's a single relation
            relation_name = relation_query.strip()
            if relation_name not in self.relations:
                raise ValueError(f"Relation '{relation_name}' not found.")

            rel_attributes = self.relations[relation_name]["attributes"]
            data = self.relations[relation_name]["data"]

        # Validate and get the indices of the attributes to project
        col_indices = []
        for attr in attributes:
            if attr not in rel_attributes:
                raise ValueError(f"Attribute '{attr}' not found in relation '{relation_query}'.")
            col_indices.append(rel_attributes.index(attr))

        # Project the required columns
        projected_data = [[row[i] for i in col_indices] for row in data]
        return [attributes] + projected_data  # Include the attributes as the header

    def crossproduct(self, query):
        # Extract relation names
        relations = query.split('(')[-1].split(')')[0].strip().split('*')
        if len(relations) != 2:
            raise ValueError("Cross product requires exactly two relations.")

        left_relation = self.get_relation_data(relations[0].strip())
        right_relation = self.get_relation_data(relations[1].strip())

        # Perform cross product
        combined_data = [left_row + right_row for left_row in left_relation['data'] for right_row in right_relation['data']]
        combined_attributes = left_relation['attributes'] + right_relation['attributes']
        return [combined_attributes] + combined_data  # Include header

    def union(self, query):
        # Extract the two relations to union
        relations = query.split('(')[-1].split(')')[0].strip().split('U')
        if len(relations) != 2:
            raise ValueError("Union requires exactly two relations.")

        left_relation = self.get_relation_data(relations[0].strip())
        right_relation = self.get_relation_data(relations[1].strip())

        # Ensure both relations have the same attributes
        if left_relation['attributes'] != right_relation['attributes']:
            raise ValueError("Union requires both relations to have the same attributes.")

        # Combine data
        combined_data = left_relation['data'] + right_relation['data']
        # Remove duplicates
        combined_data = list(map(list, set(map(tuple, combined_data))))  # Remove duplicates
        return [left_relation['attributes']] + combined_data

    def evaluate_query(self, query):
        query = query.strip()
        try:
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
            else:
                return None
        except Exception as e:
            print(f"Error evaluating query '{query}': {str(e)}")
            return None

    def difference(self, query):
        # Extract the subqueries for the difference
        subqueries = query.split(') - (')
        if len(subqueries) != 2:
            raise ValueError("Invalid difference query format")

        left_query = subqueries[0].strip() + ')'
        right_query = '(' + subqueries[1].strip()

        # Evaluate both subqueries
        left_relation = self.evaluate_query(left_query)
        right_relation = self.evaluate_query(right_query)

        # Debug: Print the two relations for comparison
        print(f"Left query: {left_query}")
        print(f"Left relation result: {left_relation}")
        print(f"Right query: {right_query}")
        print(f"Right relation result: {right_relation}")

        # Check if both relations have the same number of columns
        if not left_relation or not right_relation:
            print("One of the subqueries returned no results.")
            return None

        if len(left_relation[0]) != len(right_relation[0]):
            raise ValueError("The two relations must have the same number of columns for the difference operation.")

        # Perform set difference (rows in left_relation but not in right_relation)
        left_data = [tuple(row) for row in left_relation[1:]]  # Exclude header
        right_data = [tuple(row) for row in right_relation[1:]]  # Exclude header

        # Debug: Print the tuples being compared
        print(f"Left data tuples: {left_data}")
        print(f"Right data tuples: {right_data}")

        difference_data = list(set(left_data) - set(right_data))

        # Debug: Print the final difference result
        print(f"Difference result: {difference_data}")

        # Return result with headers
        if difference_data:
            return [left_relation[0]] + [list(row) for row in difference_data]
        else:
            return [left_relation[0]]  # Return header only if no difference found

    def get_relation_data(self, relation_name):
        # Find the relation
        if relation_name not in self.relations:
            raise ValueError(f"Relation '{relation_name}' not found.")
        return self.relations[relation_name]

    def get_attribute_index(self, attr):
        # Check each relation for the attribute
        for relation in self.relations.values():
            if attr in relation['attributes']:
                return relation['attributes'].index(attr)
        raise ValueError(f"Attribute '{attr}' not found in any relation.")

    def evaluate_condition(self, value, operator, condition_value):
        # Handle basic operators
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
