# Materialization Metrics

| dataset | algorithm | k | bytes_total | files_total | partitions_total | rows_written | materialization_time_sec | docker_root |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| cordis_horizon_inputs | kahip_fast | 2 | 342962102 | 20 | 2 | 792204 | 44.122 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | kahip_fast | 4 | 685924204 | 40 | 4 | 1584408 | 64.126 | docker_cordis_all_k4_no_rcp |
| cordis_horizon_inputs | kahip_fast | 6 | 1028886306 | 60 | 6 | 2376612 | 79.452 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | kahip_fast | 8 | 1371848408 | 80 | 8 | 3168816 | 66.198 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | metis | 2 | 342962102 | 20 | 2 | 792204 | 21.764 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | metis | 4 | 685924204 | 40 | 4 | 1584408 | 36.588 | docker_cordis_all_k4_no_rcp |
| cordis_horizon_inputs | metis | 6 | 1028886306 | 60 | 6 | 2376612 | 50.102 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | metis | 8 | 1371848408 | 80 | 8 | 3168816 | 65.514 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | parmetis | 2 | 342962102 | 20 | 2 | 792204 | 20.953 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | parmetis | 4 | 685924204 | 40 | 4 | 1584408 | 37.736 | docker_cordis_all_k4_no_rcp |
| cordis_horizon_inputs | parmetis | 6 | 1028886306 | 60 | 6 | 2376612 | 51.359 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | parmetis | 8 | 1371848408 | 80 | 8 | 3168816 | 65.609 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | ptscotch | 2 | 342962102 | 20 | 2 | 792204 | 22.235 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | ptscotch | 4 | 685924204 | 40 | 4 | 1584408 | 35.632 | docker_cordis_all_k4_no_rcp |
| cordis_horizon_inputs | ptscotch | 6 | 1028886306 | 60 | 6 | 2376612 | 48.771 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | ptscotch | 8 | 1371848408 | 80 | 8 | 3168816 | 64.245 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | rcp | 2 | 20221098 | 10 | 2 | 222406 | 2.886 | docker_cordis_materialized_rcp |
| cordis_horizon_inputs | rcp | 4 | 20221560 | 20 | 4 | 222406 | 2.972 | docker_cordis_materialized_rcp |
| cordis_horizon_inputs | rcp | 6 | 20222022 | 30 | 6 | 222406 | 2.826 | docker_cordis_materialized_rcp |
| cordis_horizon_inputs | rcp | 8 | 20222484 | 40 | 8 | 222406 | 3.287 | docker_cordis_materialized_rcp |
| cordis_horizon_inputs | scotch | 2 | 342962102 | 20 | 2 | 792204 | 21.119 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | scotch | 4 | 685924204 | 40 | 4 | 1584408 | 36.143 | docker_cordis_all_k4_no_rcp |
| cordis_horizon_inputs | scotch | 6 | 1028886306 | 60 | 6 | 2376612 | 51.015 | docker_cordis_all_k2468 |
| cordis_horizon_inputs | scotch | 8 | 1371848408 | 80 | 8 | 3168816 | 68.048 | docker_cordis_all_k2468 |
| fib25_neo4j_inputs | kahip_fast | 2 | 110505645 | 20 | 2 | 2427904 | 16.164 | docker_fib25_materialized |
| fib25_neo4j_inputs | kahip_fast | 4 | 110511815 | 40 | 4 | 2427906 | 16.552 | docker_fib25_materialized |
| fib25_neo4j_inputs | kahip_fast | 6 | 110517985 | 60 | 6 | 2427908 | 16.344 | docker_fib25_materialized |
| fib25_neo4j_inputs | kahip_fast | 8 | 110524155 | 80 | 8 | 2427910 | 16.658 | docker_fib25_materialized |
| fib25_neo4j_inputs | metis | 2 | 110505645 | 20 | 2 | 2427904 | 16.432 | docker_fib25_materialized |
| fib25_neo4j_inputs | metis | 4 | 110511815 | 40 | 4 | 2427906 | 16.651 | docker_fib25_materialized |
| fib25_neo4j_inputs | metis | 6 | 110517985 | 60 | 6 | 2427908 | 16.558 | docker_fib25_materialized |
| fib25_neo4j_inputs | metis | 8 | 110524155 | 80 | 8 | 2427910 | 16.816 | docker_fib25_materialized |
| fib25_neo4j_inputs | parmetis | 2 | 110505645 | 20 | 2 | 2427904 | 16.527 | docker_fib25_materialized |
| fib25_neo4j_inputs | parmetis | 4 | 110511815 | 40 | 4 | 2427906 | 16.251 | docker_fib25_materialized |
| fib25_neo4j_inputs | parmetis | 6 | 110517985 | 60 | 6 | 2427908 | 16.225 | docker_fib25_materialized |
| fib25_neo4j_inputs | parmetis | 8 | 110524155 | 80 | 8 | 2427910 | 16.365 | docker_fib25_materialized |
| fib25_neo4j_inputs | ptscotch | 2 | 110505645 | 20 | 2 | 2427904 | 16.997 | docker_fib25_materialized |
| fib25_neo4j_inputs | ptscotch | 4 | 110511815 | 40 | 4 | 2427906 | 17.354 | docker_fib25_materialized |
| fib25_neo4j_inputs | ptscotch | 6 | 110517985 | 60 | 6 | 2427908 | 17.523 | docker_fib25_materialized |
| fib25_neo4j_inputs | ptscotch | 8 | 110524155 | 80 | 8 | 2427910 | 16.447 | docker_fib25_materialized |
| fib25_neo4j_inputs | rcp | 2 | 65540405 | 20 | 2 |  |  | docker_fib25_materialized_rcp |
| fib25_neo4j_inputs | rcp | 4 | 63155920 | 40 | 4 |  |  | docker_fib25_materialized_rcp |
| fib25_neo4j_inputs | rcp | 6 | 64520703 | 60 | 6 |  |  | docker_fib25_materialized_rcp |
| fib25_neo4j_inputs | rcp | 8 | 64936649 | 80 | 8 |  |  | docker_fib25_materialized_rcp |
| fib25_neo4j_inputs | scotch | 2 | 110505645 | 20 | 2 | 2427904 | 16.001 | docker_fib25_materialized |
| fib25_neo4j_inputs | scotch | 4 | 110511815 | 40 | 4 | 2427906 | 16.262 | docker_fib25_materialized |
| fib25_neo4j_inputs | scotch | 6 | 110517985 | 60 | 6 | 2427908 | 16.408 | docker_fib25_materialized |
| fib25_neo4j_inputs | scotch | 8 | 110524155 | 80 | 8 | 2427910 | 16.596 | docker_fib25_materialized |
| ldbc_inputs_1_4 | kahip_fast | 2 | 853951364 | 66 | 2 | 20480521 | 103.217 | docker_ldbc_clean |
| ldbc_inputs_1_4 | kahip_fast | 4 | 853954686 | 132 | 4 | 20480521 | 104.557 | docker_ldbc_clean |
| ldbc_inputs_1_4 | kahip_fast | 6 | 853958008 | 198 | 6 | 20480521 | 105.224 | docker_ldbc_clean |
| ldbc_inputs_1_4 | kahip_fast | 8 | 853961330 | 264 | 8 | 20480521 | 113.237 | docker_ldbc_clean |
| ldbc_inputs_1_4 | metis | 2 | 853951364 | 66 | 2 | 20480521 | 115.325 | docker_ldbc_clean |
| ldbc_inputs_1_4 | metis | 4 | 853954686 | 132 | 4 | 20480521 | 115.358 | docker_ldbc_clean |
| ldbc_inputs_1_4 | metis | 6 | 853958008 | 198 | 6 | 20480521 | 117.381 | docker_ldbc_clean |
| ldbc_inputs_1_4 | metis | 8 | 853961330 | 264 | 8 | 20480521 | 112.461 | docker_ldbc_clean |
| ldbc_inputs_1_4 | parmetis | 2 | 853951364 | 66 | 2 | 20480521 | 110.249 | docker_ldbc_clean |
| ldbc_inputs_1_4 | parmetis | 4 | 853954686 | 132 | 4 | 20480521 | 110.252 | docker_ldbc_clean |
| ldbc_inputs_1_4 | parmetis | 6 | 853958008 | 198 | 6 | 20480521 | 102.693 | docker_ldbc_clean |
| ldbc_inputs_1_4 | parmetis | 8 | 853961330 | 264 | 8 | 20480521 | 103.731 | docker_ldbc_clean |
| ldbc_inputs_1_4 | ptscotch | 2 | 853951364 | 66 | 2 | 20480521 | 99.893 | docker_ldbc_clean |
| ldbc_inputs_1_4 | ptscotch | 4 | 853954686 | 132 | 4 | 20480521 | 103.714 | docker_ldbc_clean |
| ldbc_inputs_1_4 | ptscotch | 6 | 853958008 | 198 | 6 | 20480521 | 106.548 | docker_ldbc_clean |
| ldbc_inputs_1_4 | ptscotch | 8 | 853961330 | 264 | 8 | 20480521 | 105.547 | docker_ldbc_clean |
| ldbc_inputs_1_4 | rcp | 2 | 1565788587 | 64 | 2 |  |  | docker_ldbc_clean |
| ldbc_inputs_1_4 | rcp | 4 | 1939655815 | 132 | 4 |  |  | docker_ldbc_materialized_rcp |
| ldbc_inputs_1_4 | rcp | 6 | 2231587695 | 198 | 6 |  |  | docker_ldbc_materialized_rcp |
| ldbc_inputs_1_4 | rcp | 8 | 2460882970 | 264 | 8 |  |  | docker_ldbc_materialized_rcp |
| ldbc_inputs_1_4 | scotch | 2 | 853951364 | 66 | 2 | 20480521 | 109.538 | docker_ldbc_clean |
| ldbc_inputs_1_4 | scotch | 4 | 853954686 | 132 | 4 | 20480521 | 109.379 | docker_ldbc_clean |
| ldbc_inputs_1_4 | scotch | 6 | 853958008 | 198 | 6 | 20480521 | 109.475 | docker_ldbc_clean |
| ldbc_inputs_1_4 | scotch | 8 | 853961330 | 264 | 8 | 20480521 | 109.439 | docker_ldbc_clean |
| mb6_neo4j_inputs | kahip_fast | 2 | 66651574 | 20 | 2 | 1447842 | 9.362 | docker_mb6_materialized |
| mb6_neo4j_inputs | kahip_fast | 4 | 66658316 | 40 | 4 | 1447844 | 9.481 | docker_mb6_materialized |
| mb6_neo4j_inputs | kahip_fast | 6 | 66665058 | 60 | 6 | 1447846 | 9.982 | docker_mb6_materialized |
| mb6_neo4j_inputs | kahip_fast | 8 | 66671800 | 80 | 8 | 1447848 | 10.182 | docker_mb6_materialized |
| mb6_neo4j_inputs | metis | 2 | 66651574 | 20 | 2 | 1447842 | 9.524 | docker_mb6_materialized |
| mb6_neo4j_inputs | metis | 4 | 66658316 | 40 | 4 | 1447844 | 9.640 | docker_mb6_materialized |
| mb6_neo4j_inputs | metis | 6 | 66665058 | 60 | 6 | 1447846 | 10.742 | docker_mb6_materialized |
| mb6_neo4j_inputs | metis | 8 | 66671800 | 80 | 8 | 1447848 | 10.936 | docker_mb6_materialized |
| mb6_neo4j_inputs | parmetis | 2 | 66651574 | 20 | 2 | 1447842 | 9.779 | docker_mb6_materialized |
| mb6_neo4j_inputs | parmetis | 4 | 66658316 | 40 | 4 | 1447844 | 9.948 | docker_mb6_materialized |
| mb6_neo4j_inputs | parmetis | 6 | 66665058 | 60 | 6 | 1447846 | 10.380 | docker_mb6_materialized |
| mb6_neo4j_inputs | parmetis | 8 | 66671800 | 80 | 8 | 1447848 | 10.593 | docker_mb6_materialized |
| mb6_neo4j_inputs | ptscotch | 2 | 66651574 | 20 | 2 | 1447842 | 10.152 | docker_mb6_materialized |
| mb6_neo4j_inputs | ptscotch | 4 | 66658316 | 40 | 4 | 1447844 | 10.178 | docker_mb6_materialized |
| mb6_neo4j_inputs | ptscotch | 6 | 66665058 | 60 | 6 | 1447846 | 9.531 | docker_mb6_materialized |
| mb6_neo4j_inputs | ptscotch | 8 | 66671800 | 80 | 8 | 1447848 | 9.738 | docker_mb6_materialized |
| mb6_neo4j_inputs | rcp | 2 | 43412366 | 20 | 2 |  |  | docker_mb6_materialized_rcp |
| mb6_neo4j_inputs | rcp | 4 | 44975579 | 40 | 4 |  |  | docker_mb6_materialized_rcp |
| mb6_neo4j_inputs | rcp | 6 | 49766865 | 60 | 6 |  |  | docker_mb6_materialized_rcp |
| mb6_neo4j_inputs | rcp | 8 | 57148021 | 80 | 8 |  |  | docker_mb6_materialized_rcp |
| mb6_neo4j_inputs | scotch | 2 | 66651574 | 20 | 2 | 1447842 | 10.615 | docker_mb6_materialized |
| mb6_neo4j_inputs | scotch | 4 | 66658316 | 40 | 4 | 1447844 | 10.755 | docker_mb6_materialized |
| mb6_neo4j_inputs | scotch | 6 | 66665058 | 60 | 6 | 1447846 | 11.433 | docker_mb6_materialized |
| mb6_neo4j_inputs | scotch | 8 | 66671800 | 80 | 8 | 1447848 | 11.602 | docker_mb6_materialized |
