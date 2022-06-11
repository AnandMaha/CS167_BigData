# Lab 9

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) What is the schema of the file after loading it as a Dataframe

    ```text
    root
  |-- Timestamp: string (nullable = true)
  |-- Text: string (nullable = true)
  |-- Latitude: double (nullable = true)
  |-- Longitude: double (nullable = true)
    ```

* (Q2) Why in the second operation, convert, the order of the objects in the  tweetCounty RDD is (tweet, county) while in the first operation, count-by-county, the order of the objects in the spatial join result was (county, tweet)?

    ```text
    It is because of the order of the spatial joins. In the first operation, countiesRDD is left of tweetsRDD, while in the second operation, tweetsRDD is left of countiesRDD in each spatial join.
    ```

* (Q3) What is the schema of the tweetCounty Dataframe?

    ```text
    root
    |-- Timestamp: string (nullable = true)
    |-- Text: string (nullable = true)
    |-- Latitude: double (nullable = true)
    |-- Longitude: double (nullable = true)
    |-- geometry: geometry (nullable = true)
    |-- CountyID: string (nullable = true)
    ```

* (Q4) What is the schema of the convertedDF Dataframe?

    ```text
    root
    |-- CountyID: string (nullable = true)
    |-- Longitude: double (nullable = true)
    |-- Latitude: double (nullable = true)
    |-- keywords: array (nullable = true)
    |    |-- element: string (containsNull = true)
    |-- Timestamp: string (nullable = true)
    ```

* (Q5) For the tweets_10k dataset, what is the size of the decompressed ZIP file as compared to the converted Parquet file?

  | Size of the original decompressed file | Size of the Parquet file |
  | - | - |
  |  771 kb | 501 kb |

* (Q6) (Bonus) Write down the SQL query(ies) that you can use to compute the ratios as described above. Briefly explain how your proposed solution works.

    ```SQL
    -- Enter the SQL query(ies) here
    ```

    ```text
    Use this space to explain how it works.
    ```