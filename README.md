# BeyondCompare
DataComparion Tool for comparing Large files

# Technologies Used
   -	Apache Spark
   -	Java
   -	Avro
    
  
 # What is This?
  
  We offen come across situations where we need to compare large files which cannot be opened in our local machine . These files cannot be compared by an application which written in non distributed frameworks as it is time consuming. Hence an application built on a distributed framework is needed for comparision these situations. This is when this datacomparision tool comes handy.
  
  # How do we use this?
  
  This application takes 2 directories as input which need to be compared.There is anothor file by name properties that needs to be provided for the application to run. 
  The properties file has the following fields in it
    -   PrimaryKey (comma seperated if it is a composite key)
    -   FileName in the input directory that needs to be compared
    -   The delimiter that seperates the columns
    -   A Boolean value that determines if the given file has a header
    
  Using this file we can specify the list of files which need to be compared.
    
 Run the code using the below input command
 
  
  
  
  
  

