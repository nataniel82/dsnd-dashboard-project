# Import any dependencies needed to execute sql queries
import pandas as pd 
from sql_execution import QueryMixin

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
                SUM(CASE WHEN event_type = 'positive' THEN 1 ELSE 0 END) AS positive_events,
                SUM(CASE WHEN event_type = 'negative' THEN 1 ELSE 0 END) AS negative_events
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
        query_string = f"""
            SELECT
                t2.note_date,
                t2.note
            FROM
                {self.name} AS t1
            INNER JOIN
                notes AS t2 ON t1.employee_id = t2.employee_id
            WHERE
                t1.{self.name}_id = ?;
        """
        return self.pandas_query(query_string, (id,))