# DataList
DataList is a java.util.List implementation, that store items at files.
It is designed to filter and show millions items in real time.
The high speed of search and reading data is reached due to write restriction: when one thread append data, all other reads/writes failed.

[Example](src/examples/DataListExample.java)

# Highlights
+ Very fast in read-only scenario
+ Pure Java (based on java.nio FileChannel & ByteBuffer)
+ Filter indexes works on java8 lambdas (for example, age index "(Person p)->p.getAge()")
+ Each list (analog of DB table) and search result implements java.util.List<> and might be used at code, that works on lists
+ DataList could store elements compact, because elements could not be changed or removed
- Only one thread can append data, all reads until the end of the write returns error
- All the indexes are rebuilt even when adding one item
- Items could not be changed, append only. You may clear list, but should guarantee that no one read at parallel
+ Text search allows you to quickly find the indices of elements by the prefix of one or more words
+ Contains [Master/details](https://en.wikipedia.org/wiki/Master%E2%80%93detail_interface) implementation
+ Indexes and text search does not read list when search or filter data.
+ Text search for master/details find word prefixes at both tables (each prefix should be found at any table)

# Designed for
DataList was designed for stand-alone java application with predefined items. There is Master/details database with 27'000 master items and 2'630'000 details items. Rarely the base can be supplemented with new data. The user should be able to see the number of found rows in both tables during filtration.
Databases designed for high concurrency selection, update and deletion. For our purposes this is unnecessary overhead that requires a lot of time.
DataList takes about 0.5 - 1 second to filter data and append text search. Look at comparison table below.

|     Stage                    | DataList |  H2 DB   |
| ---------------------------- | --------:| --------:|
| Import first 2 million items | 22,7 sec | 29,2 sec |
| Add indexes                  |  9,7 sec | 12,4 sec |
| Add more 630 000 items       | 19,5 sec | 12,6 sec |
| Find elements ( >99% )       |  0,5 sec | 27,9 sec |
| Fetch founded items          | 12,0 sec | 15,9 sec |
| Storage size                 |  1297 Mb |  5160 Mb |

For this comparison, we add our 2'630'000 (+27'000) to Master/details DataList and H2 database.
First we import 2'000'000 items. DataList handle master items by itself, so data added from linear csv table. Half time - reading data by java.io.BufferedReader. To speed up h2 import, data separated to two lists and then imported by 200 items batches. Then we add 2 indexes: one for master table, one for details. Also we add primary key for master table.
Then we add another 630 000 items. DataList rewrites indexes at both tables, H2 - no.
Then we try to filter data by 2 criteria (both indexes are used). DataList build item indexes list at half of second (real time). It removes 2645 from details table and 64 items from master (and keeps more than 99% items). It requires 12 seconds to fully load 2 million items.
There are many ways to load data from database. We load indexes first (28 seconds) and then fetch by 200 by UID index (16 seconds).

[Comparsion code](src/examples/speed)