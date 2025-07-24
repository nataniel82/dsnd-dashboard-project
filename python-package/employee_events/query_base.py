# Import any dependencies needed to execute sql queries
import pandas as pd 
from .sql_execution import QueryMixin

# Define a class called QueryBase
# Use inheritance to add methods
# for querying the employee_events database.
class QueryBase(QueryMixin):


    # Create a class attribute called `name`
    # set the attribute to an empty string
    name= ""

    # Define a `names` method that receives
    # no passed arguments
    def names(self):
        
        # Return an empty list
        return []


    # Define an `event_counts` method
    # that receives an `id` argument
    # This method should return a pandas dataframe
    def event_counts(self, id:int):
        

        # QUERY 1
        # Write an SQL query that groups by `event_date`
        # and sums the number of positive and negative events
        # Use f-string formatting to set the FROM {table}
        # to the `name` class attribute
        # Use f-string formatting to set the name
        # of id columns used for joining
        # order by the event_date column
        query_string = f"""
            SELECT
                event_date,
                SUM(positive_events) AS SUM_POSITIVE_EVENTS,
                SUM(negative_events) AS SUM_NEGATIVE_EVENTS
            FROM
                employee_events
            WHERE
                {self.name}_id = ?
            GROUP BY
                event_date
            ORDER BY
                event_date;
        """
        return self.pandas_query(query_string, (id,))            
    

    # Define a `notes` method that receives an id argument
    # This function should return a pandas dataframe
    def notes(self, id:int):

        # QUERY 2
        # Write an SQL query that returns `note_date`, and `note`
        # from the `notes` table
        # Set the joined table names and id columns
        # with f-string formatting
        # so the query returns the notes
        # for the table name in the `name` class attribute
        if self.name == "employee":
            query_string = """
                SELECT
                    note_date,
                    note
                FROM
                    notes
                WHERE
                    employee_id = ?
                ORDER BY
                    note_date DESC;
            """
            params = (id,)  
        # Case 2: The model is 'team'. This requires a subquery.
        elif self.name == "team":
            query_string = """
                SELECT
                    n.note_date,
                    n.note
                FROM
                    notes AS n
                WHERE
                    n.employee_id IN (
                        SELECT e.employee_id
                        FROM employee AS e
                        WHERE e.team_id = ?
                    )
                ORDER BY
                    n.note_date DESC;
            """
            params = (id,)
        # Fallback for an unexpected 'name' attribute
        else:
            # Return an empty dataframe if the type is not recognized
            return pd.DataFrame({'note_date': [], 'note': []})
        # Execute the chosen query using the inherited method from the Mixin
        return self.pandas_query(query_string, params)