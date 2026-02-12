<h1>1. Data Loading Portion</h1>

<b>Krishna</b> - I made three dataframes with the proper constraints and specifications outlined by the document. 
This was the fastest way I could figure out how to do it. It is in fact slow and the follows.csv does take about a 
gigabyte of space of your RAM, so be careful opening it up. The values are fairly realistic except maybe the nickname, 
which is a random string. 

<b>AI Usage</b> - AI was used in understanding the concepts of MapReduce and also gave me the boiler template for MapReduce functions. Logic was completely done without AI.

![alt text](./Images/image.png)
![alt text](./Images/image-5.png)
![alt text](./Images/image-2.png)


<h1>2. Loading Into Hadoop</h1>

![alt text](./Images/image-3.png)
![alt text](./Images/image-4.png)


<h1>3. Queries</h1>
Installed Maven project using this command:

```shell
mvn archetype:generate -DgroupId=com.krishnagarg.proj1 -DartifactId=CircleNetAnalytics -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
```

<h1>Task A - Report the frequency of each favorite hobby (FavoriteHobby) on CircleNet</h1>

```bash
root@38fbeffd6f18:/home/ds503/shared_folder/Proj1/Queries/CircleNetAnalytics/target hdfs dfs -cat /user/root/output_A/part-r-00000 | head -n 10
Astronomy	7017
Baking	7223
Bird Watching	7080
Calligraphy	7104
Chess	6960
Cooking	7342
Cycling	7127
Dancing	7096
Fishing	7129
Gardening	7013
```

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>I made a mapper function that associates each Favorite Hobby (key) with the number one. Then in the mapper, I sum up the list of values with the associated distinct key. The key value pairs were shuffled and sorted automatically.</p>
    <h2>Optimization I Tried (Advanced Solution)</h2>
    <p>A more optimized solution would be one that implements a combiner since from the mapper a key value pair of (Reading, 1) would be going across the network to the reducer. Instead of summing up the values in the list (since they are all 1) in the reducer we could take care of the big chunk of the processing in the combiner. So I just made my reducer my combiner as well since it may or may not get used. So this is the optimal solution.</p>
    <b>Did I succeed? YES</b>
</div>

<h1>Task B - Find the 10 most popular CircleNetPages, namely, those that got the most accesses based on the ActivityLog among all pages. Return ID, NickName, and JobTitle.</h1>

![alt text](Images/part-b.png)

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>I broke this function down to three jobs. The first gets the number of instances of each page visit. It will return (PageID, # of occurrences). The second job sorts the output from the first job to find the top 10 pages. The third job gets the information for those pages from the CircleNetPage csv.</p>
    <h2>Optimization I Tried (Advanced Solution)</h2>
    <p>No optimization was necessary.</p>
    <b>Did I succeed? YES</b>
</div>


<h1>Task C - Report all CircleNetPage users (NickName, and JobTitle) whose hobby (FavoriteHobby)
is the same as your own (pick one). Note that the favorite hobby in the data file may be random sequence of characters unless you work with meaningful strings like “Reading”
or “Basketball”. This is up to you.</h1>

![alt text](Images/part-c.png)

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>This was a simple mapping problem. I filtered the users csv by a hobby, "Reading", and just returned the requested fields. This is the optimal solution as I am not using a reducer and only a mapper.</p>
    <h2>Optimization I Tried (Advanced Solution)</h2>
    <p>Didn't use a reducer and only used a mapper in the appropriate places. No need for a combiner of any kind.</p>
    <b>Did I succeed? YES</b>
</div>


<h1><br/><br/><br/>End Krishna's Part<br/><br/><br/></h1>

<br/><br/><br/>
# Preamble - Nathaniel Ince
Created my own data generation independently, unattached. All items have been run on small data sets to prove they function.

To run them, I usually open the root folder as a project in IntelliJ, use project structure settings to set up each task as an artifact and then build each task as an artifact. Then I move the artifact into my shared_folder setup.

# Utils
I wrote a small utility function to identify what file is being read.
It largely does this by using regexp to identify which of the first
entries are numbers or not numbers. This could be done as ONE regexp
instead of a string tokenizer running checks, I suppose. Not going to
implement it right now, though.
# Task D
Task d is a task that could be done as two jobs - a join and then a count. You would usually want to do this with two
jobs, but the thought hadn't honestly occurred to me.
## Unoptimized
__Mapper__: Identifies the file. If it's CircleNet, then pull out `id` and `nickname` fields by order. We write to context
our key set to `id` and value set to `"C" + nickname`.

If the file is Follows, the fields are the id of the row, the id `id1` of a person and the id `id2` of a person
that the other person is following. So we write to context with our key set to `id2` and  our value set to `"F"+id1`.

This will make more sense when we

__Reducer__: We simply go through the values and add them all to a hashset if they are followers - values that start with "F".
If the value starts with a "C" it's set to the name field. This is why we marked different values the way we did.

I'd prefer to do this with a stream and a filter, but you'd have to make the iterable into a collection which seems slow.
This process is what happens under the hood anyway.

Then we just write to context with nickname as key and size of the hashmap - the number of followers after removing possible duplicate values - as our value.

This, as desired, creates a file where each row is a name and number of followers.

Runtimes:
135909 (1)
126610 (2)

## Optimization One
__Mapper__: works the same way as unoptimized version
__Combiner__: Simply does the reduction step early - dumps the values into a hashset, then for each value in the hashset write it as value to context with the key equalling the id.
__Reducer__: Works the same way as unoptimized version.

This will have the same number of mappers and reducers, but the hope would be fewer key-value pairs in the transfer.

Runtimes:
111596 (1)
105425 (2)

Uncertain how much better things would be on a real setup with multiple nodes. Still this shows some improvement consistently.

## Optimization Two
__Mapper__: works the same way as unoptimized version
__Combiner__: In order to make more significant progress and avoid the overhead of a hashset, all we do is make the values
into a string. We get the values of the Text values and just join them with commas into one large string. So only one key-value pair is written to context.  
__Reducer__: Now we make one hashset per key, tokenize the comma-joined strings of values in Values. We go through each the same way we've done before, adding
to the hashset if it's a follower, setting it as name if it's a nickname, and writing the nickname as key and hashset size as value pair to context.

Same number of mappers and reducers, but this should reduce operations overall.

Note that for this one, the combiner can either use an ArrayLst or a Hashset. I have added an extra argument that can be "hs" to use a Hashset, anything else to use an ArrayList.

Runtimes with HashMap:
107433 (1)
107520 (2)

Runtimes with ArrayList:
105462 (1)
104534 (2)

Surprisingly, the ArrayList shows consistent better runtimes than previous. To determine if Hashmap brings
improvements consistently over previous we would need more runs.

# Task E
## Unoptimized
__Mapper__: Report the id of the person doing the action as key and who he did it to as value to context.
__Reducer__: For each id, dump all the values into an ArrayList, put the ArrayList in a Hashmap. Write to context the same id, and then as text the combination of the
size of the array (number of actions) and the size of the hashmap (how many distinct ids were action made on)

Note that it's possible that an account made NO actions whatsoever. It's not explicitly asked they be listed in the final document with 0 access to 0 distinct accounts,
so a join isn't necessary

Runtimes:
98709 (1)
98508 (2)

Already pretty fast.

## Optimization One
__Mapper__: Same as above
__Combiner__: Create a Hashmap, count each value that is in the list of values. Then write to context our key as the kay and a joined string of the value and its count for each value in the hashmap.
__Reducer__: For each key: Split the value-count pairs in the values list. Add the value to a hashset and sum the counts. The latter sum is the total number of accesses, the size of the former hashset is how many distinct accounts.
Write those values with the id as before to context.

Idea once again is to reduce the number of items being transferred, as once again I do not see a way to reduce mappers or reducers.

Runtimes:
101668 (1)
107656 (2)

So... this is not improvement. Possibly because combiners create more overhead than they would on a real setup and because of data structure creation overhead.

## Optimization Two
__Mapper__: Same as above
__Combiner__: We take the approach we did with d's second optimization and just join all the values into a string
__Reducer__: Break apart the strings, dump their values into an ArrayList and a Hashset. Former size gives us number of accesses, latter how many distinct accounts, write to context with id as key as before.

This should significantly reduce number of items being transferred as well as data structure creation overhead.

Runtimes:
94731 (1)
92790 (2)

Quite an improvement.

<br/><br/><br/>
## Ryker Germain
<h3> I also created my own data generation method for Step 1 and 2 for local testing. However, we all used Krishna's method 
for consistency. I then completed Tasks F, G, and H (methodology described below).   </h3>

<h1>Task F- Report all owners of a CircleNetPage who are more popular than an average user, namely, 
those who have more followers than the average number of followers across all owners of a CircleNetPage.</h1>

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>This problem requires 2 jobs; the first calculates the average number of followers for a user, and the second
counts the number of followers per user and filters out the users with a below average count.</p>
    <h2>Optimization I Made (Advanced Solution)</h2>
    <p>The first optimization I made was using a combiner. Rather than sending thousands of 1s across the network for
a popular user, this combines them before they are sent to the reducer. The second optimization I made was using a
distributed cache to store the global follower average as a single variable in a text file. This drastically reduces
the work done in Job 2's Reducer.</p>
</div>


<h1>Task G- Identify "outdated" CircleNetPages. Return IDs and nicknames of persons that have not accessed 
CircleNet for 90 days (i.e., no entries in the ActivityLog in the last 90 days).</h1>

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>This problem requires a couple of tasks. The first mapper gets the maximum (latest) activity timestamp each user
has performed. The second mapper reads the page dataset to match each pageID with a nickname. Finally, the reducer
checks if the latest action was within the deadline. If not, it returns the page and name.</p>
    <h2>Optimization I Made (Advanced Solution)</h2>
    <p>This problem can be optimized using a distributed cache because the Pages dataset is relatively small. 
This enables us to use a hashmap, meaning we can get rid of the reducer. We can also use a combiner in Mapper 1 to
find the local maxima per user more efficiently.</p>
</div>

<h1>Task H- Identify all those people who follow somebody’s CircleNetPage in their same RegionCode 
but are not being followed back. Report IDs and nicknames.</h1>

<div>
    <h2>What I Did (Basic Solution)</h2>
    <p>This problem checks for a reciprocal relationship between 2 pages in the same region. The mapper makes a key of 2
pages and the follows value. The reducer then checks to see if it receives inverse keys. If it does, they are mutual. If 
not, they are output. </p>
    <h2>Optimization I Made (Advanced Solution)</h2>
    <p>The simplest optimization is to have the mapper discard all pairs with differing regions. This reduces the
amount of data the reducer needs to process. The second optimization is a bit more complex. We can load the entire page
dataset into a distributed cache hashmap. This prevents the need to use a join, which would balloon due to the
repetitive follower count possibility (i.e. many-to-many relationship). </p>
</div>
