// What is a map only job?
/*
A map only job is where there 
    - no shuffle and sort
    - one output per line
    - no grouping required
    - You will filter rows
    - emit one output per record
*/

//Map Reducer Job
/*
    - shuffle and sort
    - grouping required
    - emit one output per key
    - Counting grouping ranking and joining
Common patterns:
Counting freq: 
    map: emit (word, 1)
    reduce: sum the 1s
Top-K:
    Count per key
    reduce to (key, count)
    use
        Secondary job or
        reducer side priority queue
Filtering with condition:
    if condition true:
        emit(record)
    no reduction needed
Reduce side join:
    mapper: 
        emit(joinKey, taggedRecord)
    reducer:
        group by joinKey
        separate records by tag
        combine
Distinct Counting:
    emit(key, secondaryKey)
    use Set to count unique secondaryKey
*/


/*
Column Names
    circleNetPage: id, nickname, job_title, region_code, fav_hobby
    follows: colRel, id1, id2, dateOfRel, desc
    activityLog: actionID, byWho, page_id, actionType, actionTime
*/

// Mapper
public static class MyMapper extends Mapper<LongWritable, Text, KEYOUT, VALUEOUT> {
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    context.write(keyOut, valueOut);
  }
}


// Reducer
public static class MyReducer extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
    context.write(outputKey, outputValue);
  }
}

