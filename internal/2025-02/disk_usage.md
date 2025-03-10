# Disk usage

Exploration of current disk usage in the shared directory `/sdf/data/rubin/shared/lsdb_commissioning/`

## Methods / Tools

Just this:

```
$ pip install duviz
```

## Overall

We have a 5.0T allotment, and are currently using 1.4T.

```
[                                                                                  /sdf/data/rubin/shared/lsdb_commissioning/                                                                                 ]
[____________________________________________________________________________________________________1.40TB___________________________________________________________________________________________________]
[                                                     DASH                                                     ][                        hats                       ][          tmp           ][science_pr][][]
[___________________________________________________755.87GB___________________________________________________][______________________360.01GB_____________________][________175.66GB________][_83.04GB__][][]
[                       w_2025_04                        ][                     w_2025_03                      ][                     w_2025_04                     ][        dm_48556        ][force][][]|[][]
[________________________397.81GB________________________][______________________358.06GB______________________][______________________360.01GB_____________________][________175.66GB________][48.94][][]|[][]
[            raw             ][           hats           ][           raw            ][          hats          ][                       hats                       ]|[          hats         ] [data] | ||     
[__________211.53GB__________][_________186.28GB_________][_________190.59GB_________][________167.48GB________][_____________________360.00GB_____________________]|[________175.65GB_______] [48.8] | ||     
[forcedSource][forced][sou][] [diaForcedSo][forcedS][sou]|[forcedSou][forced][sou][o] [diaForced][forcedS][sou]|[diaObject_l][diaForcedSou][object_lc][forced][sou]| [diaForcedS][forced][so]| [][]|    ||     
[__100.17GB__][55.15G][35.][] [__92.38GB__][58.99GB][30.]|[_78.93GB_][56.36G][34.][1] [_73.22GB_][60.24GB][30.]|[__96.69GB__][__96.67GB___][_71.23GB_][59.07G][32.]| [_88.54GB__][52.61G][31]| [][]|    ||     
                              [ dataset  ] [datase] [da]                              [dataset ] [datase] [da]  [ dataset  ] [  dataset  ] [dataset ] [datas] [da]   [ dataset ] [datas] [d]   []|||           
                              [_92.28GB__] [58.89G] [30]                              [73.12GB_] [60.14G] [29]  [_96.28GB__] [__95.06GB__] [71.13GB_] [58.96] [32]   [_86.93GB_] [52.51] [3]   []|||       
```

Notes:

* We use around 400G for each weekly release. 
* The `raw` tends to be larger, likely because:
    * this includes columns we later remove
    * once fields are nested, we can remove duplicate fields
    * we use fewer total files, and are able to benefit from parquet economies of scale
* We've been keeping the `raw` directories around as we prototype DASH, but can likely remove all of the older `raw` directories.

## Slicing data

**By raw vs catalog**

There's about a 14% size reduction, when we go from raw to catalogs (404.12/353.76).

**By dataset type**

Cells here only show 4 dataset types. This is because the other two dataset types (diaObject and diaSource) are so small that their size is basically a rounding error.

This is using the older "DASH" data, but still nice to see the numbers.

breakdown (in GB)

| dataset type    | raw:w_2025_03 | raw:w_2025_04 | hats:w_2025_03 | hats:w_2025_04 |
| --------------- | ------------- | ------------- | -------------- | -------------- |
| object          | 18.52         | 18.38         | 2.23           | 2.22           |
| source          | 34.83         | 35.63         | 30.01          | 30.75          |
| diaForcedSource | 78.93         | 100.17        | 73.22          | 92.38          |
| forcedSource    | 56.36         | 55.15         | 60.24          | 58.99          |

breakdown (in percent)

| dataset type    | raw:w_2025_03 | raw:w_2025_04 | hats:w_2025_03 | hats:w_2025_04 |
| --------------- | ------------- | ------------- | -------------- | -------------- |
| object          | 9.82          | 8.78          | 1.35           | 1.2            |
| source          | 18.46         | 17.02         | 18.11          | 16.68          |
| diaForcedSource | 41.84         | 47.85         | 44.19          | 50.11          |
| forcedSource    | 29.88         | 26.35         | 36.35          | 32             |


## Considering inodes

duviz also allows showing the number of inodes consumed.

```
[____________________________________________________________________________________________________43.01k___________________________________________________________________________________________________]
[                                                                                        DASH                                                                                        ][  hats ][scien][tmp][]| 
[_______________________________________________________________________________________37.94k_______________________________________________________________________________________][_1.94k_][1.36k][1.1][]| 
[                                        w_2025_04                                        ][                                       w_2025_03                                        ] [w_2025] [or][] [dm] |   
[__________________________________________18.99k_________________________________________][_________________________________________18.95k_________________________________________] [1.94k_] [84][] [1.] |   
[                                         raw                                         ][h] [                                        raw                                         ][h]  [ hats]  [h] |  [h]      
[________________________________________18.33k_______________________________________][6] [_______________________________________18.31k_______________________________________][6]  [1.91k]  [7] |  [1]      
[                                sourceTable                                 ][o][][f] ||  [                                sourceTable                                ][o][][f] ||   [][]||   ||     ||       
[___________________________________16.47k___________________________________][6][][5] ||  [___________________________________16.47k__________________________________][5][][5] ||   [][]||   ||     ||       
                                                                                                                                                                                      | |                      
                                                                                                                                                                                      | |   
                                                                                                                                                                        
```

Notes:
* raw sourceTable consistently uses most of the inodes, as the original butler data is stored so granularly.

## Recommendations 

**for today**

Delete the following directories (relative to `/sdf/data/rubin/shared/lsdb_commissioning`):

* `tmp/`
* `DASH/`

**for policy**

* Keep the last two successful hats-ifications
* Keep the raw data from the most recent hats-ification
