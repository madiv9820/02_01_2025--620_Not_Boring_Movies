# 620. Not Boring Movies

- ### Intuition:
    The goal is to filter and sort a dataset based on certain conditions. Specifically, we need to:

    1. **Filter out unwanted entries**: We want to exclude data that doesn't meet certain criteria (e.g., movies with descriptions labeled as "boring").
    2. **Apply a mathematical or logical condition**: We need to ensure that certain data entries meet specific rules (e.g., selecting only those entries where the identifier is odd).
    3. **Sort the data**: Once the dataset is filtered, it needs to be ordered based on a specific attribute (e.g., movie ID, ordered in descending order so the largest IDs come first).

- ### Approach (Common Across SQL, PySpark, and Pandas):
    1. **Filtering**:
        - Identify the conditions that must be met for an entry to be considered relevant. These conditions could involve:
            - String matching (e.g., excluding rows where the description is 'boring').
            - Mathematical conditions (e.g., selecting rows where the ID is odd).
        - Combine these conditions logically (often with an AND or OR) to filter the dataset down to only the rows that meet all the required criteria.
    
    2. **Sorting**:
        - Once the dataset is filtered, sorting helps in organizing the entries for better readability or further analysis.
        - Identify which column or set of columns you want to sort by (e.g., sorting by ID in descending order).
        - Sorting can often be done in ascending or descending order depending on the requirement.

    3. **Output**:
        - After filtering and sorting the data, the final step is to return the processed dataset. This dataset now contains only the entries that meet the filtering criteria, organized in the desired order.

- ### Detailed Steps:
    1. **Select the columns** you are interested in for filtering and sorting. This will depend on the structure of your dataset and the specific analysis you are trying to perform.
    
    2. **Filter the rows** based on your conditions:
        - For string conditions (e.g., "not 'boring'"), check if the value of the column is equal to, not equal to, or contains certain keywords.
        - For numerical conditions (e.g., "odd number"), apply the necessary mathematical operation (like modulo for checking odd/even numbers).

    3. **Sort the data** according to the chosen column:
        - Sorting can be done in ascending or descending order based on the requirements of the task. For example, sorting by the ID of movies in descending order ensures that higher-numbered movies appear first.

    4. **Return the processed dataset**:
        - Once the dataset is filtered and sorted, return it as the output. The returned dataset will now contain only the relevant data points, arranged in the desired order.

- ### Common Elements Across SQL, PySpark, and Pandas:
    - **Filtering**: In all three technologies, filtering involves applying logical conditions to the dataset (e.g., `description != 'boring'` or `id % 2 == 1`).
    - **Sorting**: After filtering, sorting involves ordering the dataset based on a specific column, with an option for ascending or descending order.
    - **Output**: The result of these operations is typically returned as a new dataset or view that contains only the relevant, sorted data.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT * FROM CINEMA
        WHERE description != 'boring' AND id % 2 = 1  -- Filter: description is not 'boring' and id is odd
        ORDER BY rating DESC  -- Order the results by rating in descending order
        ```
    - **PySpark:**
        ```python3 []
        def not_boring_movies(cinema: spark_DataFrame) -> spark_DataFrame:
            # Apply a filter to the 'cinema' DataFrame to select rows where:
            # 1. The 'description' column is not 'boring'
            # 2. The 'id' column is an odd number (id % 2 == 1)
            not_Boring_Movies = cinema.filter(
                                        # Filter out 'boring' descriptions
                                        (cinema['description'] != 'boring') &  
                                        # Include only rows where 'id' is odd
                                        (cinema['id'] % 2 == 1))\
                                    .orderBy(
                                        # Order the filtered rows by 'rating' 
                                        # in descending order (i.e., higher ids first)
                                        cinema.rating.desc())  

            # Return the filtered and ordered DataFrame containing non-boring movies with odd ids
            return not_Boring_Movies
        ```
    - **Pandas:**
        ```python3 []
        def not_boring_movies(cinema: pd.DataFrame) -> pd.DataFrame:
            # Filter the 'cinema' DataFrame to select rows where:
            # 1. The 'description' column is not equal to 'boring'
            # 2. The 'id' column is an odd number (id % 2 == 1)
            not_Boring_Movies = cinema[
                (cinema['description'] != 'boring') &   # Condition: exclude movies with 'boring' in description
                (cinema['id'] % 2 == 1)                 # Condition: include only rows where 'id' is odd
            ]
            
            # Sort the filtered DataFrame by the 'rating' column in descending order (highest rating first)
            # The 'inplace=True' argument modifies the DataFrame in place, so no need to assign it back.
            not_Boring_Movies.sort_values(by = ['rating'], ascending = False, inplace = True)
            
            # Return the filtered and sorted DataFrame containing non-boring movies with odd ids
            return not_Boring_Movies
        ```