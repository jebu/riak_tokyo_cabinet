{application, riak_tokyo_cabinet,
 [
  {description, "A Tokyo Cabinet for Riak based on Toke"},
  {vsn, "1.0"},
  {modules, [
             riak_tokyo_cabinet_app,
             riak_tokyo_cabinet_sup,
             riak_kv_tokyo_cabinet_backend,
             toke
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { riak_tokyo_cabinet_app, []}},
  {env, [
      {data_root, "data/tokyo_cabinet"},
      {records_to_cache, 500000},
      {bucket_size, 1000000},
      {hdb_apow, 5},
      {hdb_fpow, 15},
      {hdb_options, [large]},
      {hdb_open_options, [read, write, create]}
  ]}
 ]}.
