Feature: Behavorial Test on STG_GERMANY DataMart

  Scenario: Create and verify tables structure
    Given that a Matillion Script to create tables in Snowflake is executed
    When I check the tables created in Snowflake
    Then the tables are correctly created


  Scenario: Verify that the data inserted in the tables is correct
    Given that a Matillion Script to insert data into the tables in Snowflake is executed
    When I check the data of the tables in Snowflake
    Then the data inserted is correct


  Scenario: Delete all tables created
    Given that all the tables created in Snowflake for the test are dropped
    When I check whether the tables exist in Snowflake
    Then the data is deleted correctly