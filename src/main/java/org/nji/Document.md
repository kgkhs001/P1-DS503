# Preamble
Created my own data generation independently, unattached. All items have been run on small data sets to prove they function.

# Utils
I wrote a small uitlity function to identify what file is being read.
It largely does this by using regexp to identify which of the first
entries are numbers or not numbers. This could be done as ONE regexp
instead of a string tokenizer running checks, I suppose. Not going to
implement it right now, though.
# Task d
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

I'd prefer to do this with a stream and a filter but you'd have to make the iterable into a collection which seems slow.
This process is what happens under the hood anyway.

Then we just write to context with nickname as key and size of the hashmap - the number of followers after removing possible duplicate values - as our value.

This, as desired, creates a file where each row is a name and number of followers.

Runtimes:
135909 (1)
126610 (2)

## Optimization one
__Mapper__: works the same way as unoptimized version
__Combiner__: Simply does the reduction step early - dumps the values into a hashset, then for each value in the hashset write it as value to context with key equalling the id.
__Reducer__: Works the same way as unoptimized version.

This will have the same number of mappers and reducers, but the hope would be fewer key-value pairs in the transfer.

Runtimes:
111596 (1)
105425 (2)

Unecertain how much better things would be on a real setup with multiple nodes. Still this shows some improvement consistently.

## Optimization two
__Mapper__: works the same way as unoptimized version
__Combiner__: In order to make more significant progress and avoid the overhead of a hashset, all we do is make the values
into a string. We get the values of the Text values and just join them with commas into one large string. So only one key-value pair is written to context.  
__Reducer__: Now we make one hashset per key, tokenize the comma-joined strings of values in Values. We go through each the same way we've done before, adding
to the hashset if it's a follower, setting it as name if it's a nickname, and writing the nickname as key and hashset size as value pair to context.

Same number of mappers and reducers, but this should reduce operations overall.

Note that for this one, the combienr can either use an ArrayLst or a Hashset. I have added an extra argument that can be "hs" to use a Hashset, anything else to use an ArrayList.

Runtimes with HashMap:
107433 (1)
107520 (2)

Runtimes with ArrayList:
105462 (1)
104534 (2)

Surprisingly, the ArrayList shows consistent better runtimes than previous. To determine if Hashmap brings
improvements consistently over previous we would need more runs.

# Task e
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

## Optimization one
__Mapper__: Same as above
__Combiner__: Create a Hashmap, count each value that is in the list of values. Then write to context our key as the kay and a joined string of the value and its count for each value in the hashmap.
__Reducer__: For each key: Split the value-count pairs in the values list. Add the value to a hashset and sum the counts. The latter sum is the total number of accesses, the size of the former hashset is how many distinct accounts.
Write those values with the id as before to context.

Idea once again is to reduce the number of items being transferred, as once again I do not see a way to reduce mappers or reducers.

Runtimes:
101668 (1)
107656 (2)

So... this is not improvement. Possibly because combiners create more overhead than they would on a real setup and because of data structure creation overhead.

## Optimization two
__Mapper__: Same as above
__Combiner__: We take the approach we did with d's second optimization and just join all the values into a string
__Reducer__: Break apart the strings, dump their values into an ArrayList and a Hashset. Former size gives us number of accesses, latter how many distinct accounts, write to context with id as key as before.

This should significantly reduce number of items being transferred as well as data structure creation overhead.

Runtimes:
94731 (1)
92790 (2)

Quite an improvement.