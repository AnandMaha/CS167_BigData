# Lab 7

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) What is your command?
  - mongoimport -d db -c contacts --file C:\ProgrammingFolder\CS167-BigData\workspace\amaha018_lab7\contacts.json --jsonArray
* (Q2) What is the output of the above command?
  - 2022-05-13T17:22:34.094-0700    connected to: mongodb://localhost/
2022-05-13T17:22:34.109-0700    10 document(s) imported successfully. 0 document(s) failed to import.
* (Q3) What is your command?
  - db.contacts.find().sort({Name: 1})
* (Q4) What is your command?
  - db.contacts.find({}, {Name:1}).sort({Name: -1})
* (Q5)  Is the comparison of the attribute `Name` case-sensitive?
  - Yes, it is case-sensitive. I insert the document, {Name: "aguirre Fox"} with the command, db.contacts.insert({Name: "aguirre Fox"}). Because it appears first rather than last in the descending sort, the comparison is case-sensitive because lower-case "a" has a higher ascii value than any upper-case letter. I then deleted this document with the command, db.contacts.deleteOne({Name: "aguirre Fox"}).
* (Q6)  What is your command?
  - db.contacts.find({}, {Name:1, _id:0}).sort({Name: -1})
* (Q7)  Does MongoDB accept this document while the `Name` field has a different type than other records?
  - Yes, MongoDB does accept it. 
* (Q8) What is your command?
  - db.contacts.insert({Name: {First:"David", Last: "Bark"}})
* (Q9) What is the output of the above command?
  - WriteResult({ "nInserted" : 1 })
* (Q10)  Where do you expect the new record to be located in the sort?
  - I expect it to be first in the sort order as it is type object and since type object is larger in terms of comparison, it will show up first above the string types in descending order. And indeed when running the (Q4) command, it does appear first.
* (Q11) What is your command?
  - db.contacts.insert({Name: ["David", "Bark"]})
* (Q12) What is the output of the above command?
  - WriteResult({ "nInserted" : 1 })
* (Q13) Where do you expect the new document to appear in the sort order. Verify your answer and explain after running the query.
  - I expect it to be wherever "David" would be sorted in descending order among the strings because in descending order the array would pick out its largest element, which in this case would be "David". And indeed when running the (Q4) command, this new document appears between the Names "Hayes Weaver" and "Craft Parks" exactly where the normal string "David" would be.
* (Q14)  Where do you expect the last inserted record, `{Name: ["David", "Bark"]}` to appear this time? Does it appear in the same position relative to the other records? Explain why or why not.
  - I expect it to be wherever "Bark" would be sorted in ascending order among the strings because in ascending order the array would pick out its smallest element, which in this case would be "Bark". And indeed when running the modified (Q4) command, this new document appears between the Names "Aimee Mcintosh" and "Cooke Schroeder" exactly where the normal string "Bark" would be. 
  - (Q4) modified command: db.contacts.find({}, {Name:1}).sort({Name: 1})
* (Q15) Is MongoDB able to build the index on that field with the different value types stored in the `Name` field?
  - Yes, MongoDB was able to build the index of the field with the different value types. 
* (Q16) What is your command?
  - db.contacts.createIndex({"Name":1})
* (Q17) What is the output of the above command?
  - {
            "numIndexesBefore" : 1,
            "numIndexesAfter" : 2,
            "createdCollectionAutomatically" : false,
            "ok" : 1
    }