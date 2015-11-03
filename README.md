# graphx
Hello
Graphx Test

A note on VertexID
https://issues.apache.org/jira/browse/SPARK-1153?jql=text%20~%20%22vertexid%22

test

V1
---

- EdgeAttribute   
-- DataSource (Where the link came from (what file/etc) )  
-- LinkDateTime ( When the link was made )  


- VertexAttribute  
-- VertexType (Cookie/Email/etc)  
-- VertexValue ( joe@foo.com )  


- Cardinality Constraints
-- Many to 1, 1 to 1, Many to Many
--- If constraint is violated. do not use .. Vertex? Edged? Both? Which?

- Pollution Constraints
-- Cookies for example becomes polluted (if we all buy stuff using the same computer)
We can first find the Connected Component, then discard links if they are polluted
We are going to ignore time based for V1

Given a Connected Component with N Accounts, find .. Common Ancestor along shortest path?) and Prune
For POC, measure how often polluted
What is best/worst case

