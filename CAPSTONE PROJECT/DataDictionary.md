# Data Engineering - Capstone project
## Data Dictionary

### Airport Data by State
- country: string (nullable = true)-United States
- state: string (nullable = true)-State of the U.S.
- avg_elevation_ft: double (nullable = true)-Average elevation based on all airport data from each state.

### U.S. Demographic by State
- State: string (nullable = true)-Full state name
- state_code: string (nullable = true)-Abbreviated state code
- median_age: double (nullable = true)-Median Age per state
- pct_male_pop: double (nullable = true)- % Avg Male population per state
- pct_female_pop: double (nullable = true)-% Avg Female population per state
- pct_veterans: double (nullable = true)-% Avg Veteran population per state
- pct_foreign_born: double (nullable = true)-% Avg Foreign-Born population per state
- native_american: double (nullable = true)-% Avg Native American population per state
- Asian: double (nullable = true)-% Avg Asian population per state
- hispanic_or_latino: double (nullable = true)% Avg Hispanic or Latino population per state
- Black: double (nullable = true)-% Avg Black population per state
- White: double (nullable = true)-% Avg White population per state

### Immigration Data by State with Origin
- cicid: double (nullable = true)-ID number of each individual
- year: integer (nullable = true)-Year of Immigration
- month: integer (nullable = true)-Month of Immigration
- origin_country: string (nullable = true)-Country of Origin
- i94port: string (nullable = true)-City Port Code where Immigrant entered
- city_port_name: string (nullable = true)-City Port Name
- state_code: string (nullable = true)-Abbreviated State code
- dest_state_name: string (nullable = true)-State Name
### Temperature Data by State
- year: integer (nullable = true)-Temperature Year
- month: integer (nullable = true)-Temerpature Month
- avg_temp_celcius: double (nullable = true)-Avg Temperature in Celcius per State
- avg_temp_fahrenheit: double (nullable = true)-Avg Temperatrue in Fahrenheit
- state_abbrev: string (nullable = true)-Abbreviated State Code
- State: string (nullable = true)-State Name
- Country: string (nullable = true)-United States
### Fact Table
- year: integer (nullable = true)-Year from immigration table
- immig_month: integer (nullable = true)-Month from immigration table
- immig_origin: string (nullable = true)-Country of Origin from immigration table
- to_immig_state: string (nullable = true)-State immigrated to from immigration table
- to_immig_state_count: long (nullable = false)-Total count of people immigrated per state from immigration table
- avg_temp_fahrenheit: double (nullable = true)-Avg temperature per state from Temperature table
- avg_elevation_ft: double (nullable = true)-Avg elevation per state from Airport table
- pct_foreign_born: double (nullable = true)-Avg % foreign born from Demographic table
- native_american: double (nullable = true)-Avg % Native American opulation from Demographic table
- Asian: double (nullable = true)-Avg % Asian population from Demographic table
- hispanic_or_latino: double (nullable = true)-% Avg Hispanic or Latino population per state from Demographic table
- Black: double (nullable = true)-% Avg Black population per state from Demographic table
- White: double (nullable = true)-% Avg White population per state from Demographic table
